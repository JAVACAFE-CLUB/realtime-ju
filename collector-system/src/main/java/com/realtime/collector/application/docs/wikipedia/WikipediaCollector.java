package com.realtime.collector.application.docs.wikipedia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonGenerator;
import com.realtime.collector.application.docs.wikipedia.dto.ShardStats;
import com.realtime.collector.application.docs.wikipedia.dto.WikiManifest;
import com.realtime.collector.application.docs.wikipedia.dto.WikiPage;
import com.realtime.collector.application.docs.wikipedia.util.WikiXmlUtil;
import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
import com.realtime.collector.infrastructure.messaging.CollectorEventPublisher;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.util.CollectionIdGenerator;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPOutputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikipediaCollector {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventPublisher collectorEventPublisher;

    @Async("wikiTaskExecutor")
    public CompletableFuture<Void> collectFromLocalDump(Path dumpPath, String lang, String dumpDate) {
        String collectionId = CollectionIdGenerator.generateId("WIKI");
        try (InputStream is = openMaybeCompressed(dumpPath)) {
            ShardStats stats = streamParseAndUploadShards(is, lang, dumpDate, collectionId);
            String manifestUrl = storeManifest(collectionId, lang, dumpDate, stats);
            saveCollectionMetadata(collectionId, manifestUrl, lang, dumpDate);
            collectorEventPublisher.publishCollected(
                    ContentSource.DOCS_WIKIPEDIA.name(),
                    KafkaTopics.RAW_DOCS_WIKIPEDIA,
                    collectionId,
                    manifestUrl,
                    stats.getPagesTotal()
            );
            log.info("Wikipedia 수집 완료 - collectionId={}, pagesTotal={}, shardsTotal={}", collectionId,
                    stats.getPagesTotal(), stats.getShardsTotal());
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            log.error("Wikipedia 수집 실패 - collectionId={}, lang={}, dumpDate={}, dumpPath={}",
                    collectionId, lang, dumpDate, dumpPath, e);
            boolean retriable = isRetriable(e);
            String topic = retriable ? KafkaTopics.RAW_DOCS_WIKIPEDIA_RETRY : KafkaTopics.RAW_DOCS_WIKIPEDIA_DLQ;
            collectorEventPublisher.publishCollectError(
                    ContentSource.DOCS_WIKIPEDIA.name(), topic, collectionId, "", "INTERNAL_SERVER_ERROR",
                    e.getMessage(), retriable
            );
            return CompletableFuture.failedFuture(e);
        }
    }

    private InputStream openMaybeCompressed(Path path) throws IOException {
        InputStream fis = new BufferedInputStream(Files.newInputStream(path));
        String name = path.getFileName() != null ? path.getFileName().toString() : "";
        if (name.endsWith(".bz2") || name.endsWith(".bz")) {
            return new BZip2CompressorInputStream(fis, true);
        }
        return fis;
    }

    // SAX/StAX로 <page> 단위 스트리밍 파싱하여 NDJSON GZIP 샤드 업로드
    private ShardStats streamParseAndUploadShards(InputStream is, String lang, String dumpDate, String collectionId)
            throws Exception {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        XMLStreamReader reader = factory.createXMLStreamReader(is, "UTF-8");

        int pagesPerShard = 5000; // 기본 샤드 단위(페이지 수)
        String prefix = createBasePrefix(lang, dumpDate, collectionId);
        int shardIndex = 0;
        ShardWriter shard = null;

        ShardStats stats = new ShardStats();
        stats.setLang(lang);
        stats.setDumpDate(dumpDate);
        stats.setShardKeys(new ArrayList<>());

        WikiPage page = null;
        String currentElement = null;
        String currentParent = null;
        StringBuilder textBuffer = null;

        try {
            while (reader.hasNext()) {
                int event = reader.next();
                switch (event) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        String name = reader.getLocalName();
                        currentElement = name;
                        if ("page".equals(name)) {
                            page = new WikiPage();
                        } else if ("revision".equals(name)) {
                            currentParent = "revision";
                        } else if ("contributor".equals(name)) {
                            currentParent = "contributor";
                        } else if ("redirect".equals(name) && page != null) {
                            String titleAttr = getAttribute(reader, "title");
                            if (titleAttr != null) {
                                page.setRedirectTitle(titleAttr);
                            }
                        }
                        // 값이 필요한 요소는 버퍼 초기화
                        if (page != null && ("title".equals(name)
                                || "text".equals(name)
                                || "username".equals(name)
                                || "ns".equals(name)
                                || "id".equals(name)
                                || "timestamp".equals(name))) {
                            textBuffer = new StringBuilder(1024);
                        }
                    }
                    case javax.xml.stream.XMLStreamConstants.CHARACTERS, javax.xml.stream.XMLStreamConstants.CDATA -> {
                        if (page != null && currentElement != null && textBuffer != null && !reader.isWhiteSpace()) {
                            textBuffer.append(reader.getText());
                        }
                    }
                    case javax.xml.stream.XMLStreamConstants.END_ELEMENT -> {
                        String name = reader.getLocalName();

                        if (page != null && textBuffer != null && currentElement != null && currentElement.equals(
                                name)) {
                            String value = textBuffer.toString();
                            switch (name) {
                                case "title" -> page.setTitle(value);
                                case "ns" -> page.setNs(
                                        WikiXmlUtil.parseIntSafe(
                                                value));
                                case "id" -> {
                                    if ("revision".equals(currentParent)) {
                                        page.setRevisionId(value);
                                    } else if (currentParent == null) {
                                        // 페이지 최상위 id
                                        page.setPageId(value);
                                    } else if ("contributor".equals(currentParent)) {
                                        // contributor id는 현재 스키마에 저장하지 않음
                                    }
                                }
                                case "timestamp" -> page.setTimestamp(value);
                                case "username" -> page.setContributor(value);
                                case "text" -> page.setText(value);
                            }
                        }

                        // clear current element buffer
                        if (textBuffer != null && name.equals(currentElement)) {
                            textBuffer = null;
                        }

                        if ("contributor".equals(name) || "revision".equals(name)) {
                            currentParent = null;
                        }

                        if ("page".equals(name) && page != null) {
                            // 샤드 오픈
                            if (shard == null || shard.pagesInShard >= pagesPerShard) {
                                if (shard != null) {
                                    shard.close();
                                    uploadShard(prefix, shard, collectionId, lang, dumpDate, stats);
                                }
                                shard = ShardWriter.openTemp(prefix, shardIndex++, objectMapper);
                            }

                            // NDJSON 한 줄 기록 (null-safe)
                            Map<String, Object> json = new LinkedHashMap<>();
                            json.put("collectionId", collectionId);
                            json.put("lang", lang);
                            json.put("dumpDate", dumpDate);
                            json.put("pageId", page.getPageId() != null ? page.getPageId() : "");
                            json.put("title", page.getTitle() != null ? page.getTitle() : "");
                            json.put("ns", page.getNs() != null ? page.getNs() : -1);
                            json.put("redirectTitle", page.getRedirectTitle() != null ? page.getRedirectTitle() : "");
                            json.put("revisionId", page.getRevisionId() != null ? page.getRevisionId() : "");
                            json.put("timestamp", page.getTimestamp() != null ? page.getTimestamp() : "");
                            json.put("contributor", page.getContributor() != null ? page.getContributor() : "");
                            json.put("text", page.getText() != null ? page.getText() : "");
                            objectMapper.writeValue(shard.jsonGen, json);
                            shard.jsonGen.writeRaw('\n');

                            shard.pagesInShard++;
                            stats.setPagesTotal(stats.getPagesTotal() + 1);
                            page = null;
                        }
                        currentElement = null;
                    }
                    default -> {
                        // ignore
                    }
                }
            }
        } finally {
            try {
                reader.close();
            } catch (Exception ignore) {
            }
        }

        if (shard != null) {
            shard.close();
            uploadShard(prefix, shard, collectionId, lang, dumpDate, stats);
        }

        return stats;
    }

    private void uploadShard(String prefix, ShardWriter shard, String collectionId,
                             String lang, String dumpDate, ShardStats stats) throws Exception {
        long size = Files.size(shard.tempPath);
        String shardKey = String.format("%s/part-%05d.ndjson.gz", prefix, shard.shardIndex);
        try (InputStream in = Files.newInputStream(shard.tempPath)) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_DOCS_WIKIPEDIA)
                    .object(shardKey)
                    .stream(in, size, -1)
                    .contentType("application/x-ndjson")
                    .userMetadata(Map.of(
                            "collection-id", collectionId,
                            "lang", lang,
                            "dump-date", dumpDate,
                            "pages", String.valueOf(shard.pagesInShard)
                    ))
                    .build());
        }
        stats.setBytesTotal(stats.getBytesTotal() + size);
        stats.setShardsTotal(stats.getShardsTotal() + 1);
        stats.getShardKeys().add(shardKey);
        try {
            Files.deleteIfExists(shard.tempPath);
        } catch (IOException ignore) {
        }
    }

    private String storeManifest(String collectionId, String lang, String dumpDate, ShardStats stats) throws Exception {
        String prefix = createBasePrefix(lang, dumpDate, collectionId);
        WikiManifest manifest = new WikiManifest();
        manifest.setCollectionId(collectionId);
        manifest.setLang(lang);
        manifest.setDumpDate(dumpDate);
        manifest.setPagesTotal(stats.getPagesTotal());
        manifest.setShardsTotal(stats.getShardsTotal());
        manifest.setBytesTotal(stats.getBytesTotal());
        manifest.setShards(stats.getShardKeys());
        manifest.setSchemaVersion("1");
        manifest.setCreatedAt(Instant.now().toString());

        byte[] bytes = objectMapper.writeValueAsBytes(manifest);
        String key = String.format("%s/manifest.json", prefix);
        minioClient.putObject(PutObjectArgs.builder()
                .bucket(MinIOBuckets.RAW_DOCS_WIKIPEDIA)
                .object(key)
                .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                .contentType("application/json; charset=utf-8")
                .userMetadata(Map.of(
                        "collection-id", collectionId,
                        "schema-version", manifest.getSchemaVersion()
                ))
                .build());
        return String.format("minio://%s/%s", MinIOBuckets.RAW_DOCS_WIKIPEDIA, key);
    }

    private void saveCollectionMetadata(String collectionId, String manifestUrl, String lang, String dumpDate) {
        ContentMetadata meta = ContentMetadata.builder()
                .source(ContentSource.DOCS_WIKIPEDIA.code())
                .externalId(collectionId)
                .title(String.format("Wikipedia Dump %s %s", lang, dumpDate))
                .rawUri(manifestUrl)
                .collectionId(collectionId)
                .collectedAt(LocalDateTime.now())
                .build();
        contentMetadataRepository.saveAll(List.of(meta));
    }

    private boolean isRetriable(Exception e) {
        return true;
    }

    private String createBasePrefix(String lang, String dumpDate, String collectionId) {
        // 위키는 덤프일 기준: yyyy/MM/dd/{collectionId}
        java.time.LocalDate dt = parseDumpDateOrToday(dumpDate);
        String day = dt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        return String.format("%s/%s", day, collectionId);
    }

    private static java.time.LocalDate parseDumpDateOrToday(String dumpDate) {
        try {
            if (dumpDate != null && dumpDate.length() == 8) {
                return java.time.LocalDate.parse(dumpDate, java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            }
        } catch (Exception ignore) {
        }
        return java.time.LocalDate.now();
    }

    private static class ShardWriter implements AutoCloseable {
        final int shardIndex;
        final Path tempPath;
        final GZIPOutputStream gzipOut;
        final JsonGenerator jsonGen;
        int pagesInShard = 0;

        private ShardWriter(int shardIndex, Path tempPath, GZIPOutputStream gzipOut, JsonGenerator jsonGen) {
            this.shardIndex = shardIndex;
            this.tempPath = tempPath;
            this.gzipOut = gzipOut;
            this.jsonGen = jsonGen;
        }

        static ShardWriter openTemp(String prefix, int shardIndex, ObjectMapper objectMapper) throws IOException {
            Path temp = Files.createTempFile("wiki-shard-" + shardIndex + "-",
                    ".ndjson.gz");
            OutputStream fos = Files.newOutputStream(temp);
            GZIPOutputStream gz = new GZIPOutputStream(
                    new java.io.BufferedOutputStream(fos), true);
            JsonGenerator gen;
            try {
                gen = objectMapper.getFactory().createGenerator(gz);
                gen.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            } catch (Exception e) {
                try { gz.close(); } catch (IOException ignore) {}
                throw e;
            }
            return new ShardWriter(shardIndex, temp, gz, gen);
        }

        public void close() throws IOException {
            try {
                jsonGen.flush();
                jsonGen.close(); // AUTO_CLOSE_TARGET 비활성화 상태이므로 gzipOut은 닫히지 않음
            } catch (Exception ignore) {}
            gzipOut.flush();
            gzipOut.finish();
            gzipOut.close();
        }
    }

    private static String getAttribute(XMLStreamReader reader, String localName) {
        return WikiXmlUtil.getAttribute(reader, localName);
    }
}
