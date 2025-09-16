package com.realtime.collector.application.docs.wikipedia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.collector.application.docs.wikipedia.dto.ShardStats;
import com.realtime.collector.application.docs.wikipedia.dto.WikiManifest;
import com.realtime.collector.application.docs.wikipedia.util.WikiParsingContext;
import com.realtime.collector.application.docs.wikipedia.util.WikiXmlParser;
import com.realtime.collector.application.docs.wikipedia.util.WikiXmlUtil;
import com.realtime.collector.application.util.CollectorEventAsyncInvoker;
import com.realtime.collector.application.util.RetryUtils;
import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.exception.MinioStorageException;
import com.realtime.common.util.CollectionIdGenerator;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;


/**
 * 로컬 Wikipedia 덤프(XML 또는 bzip2 압축)를 파싱하여 NDJSON 샤드로 업로드하고, 매니페스트를 생성/저장한 뒤 수집 메타데이터와 이벤트를 발행하는 수집기입니다.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class WikipediaCollector {

    private static final String WIKI_COLLECTION_PREFIX = "WIKI";
    private static final String SCHEMA_VERSION = "1";
    private static final DateTimeFormatter DUMP_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter PREFIX_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventAsyncInvoker eventAsyncInvoker;
    private final WikiXmlParser xmlParser;

    @Value("${collector.wikipedia.pages-per-shard:5000}")
    private int pagesPerShard;

    /**
     * 비동기로 로컬 덤프 파일을 파싱/업로드합니다.
     *
     * @param dumpPath 덤프 파일 경로(bz2 지원)
     * @param lang     언어 코드(예: ko, en)
     * @param dumpDate 덤프 기준일(yyyyMMdd)
     */
    @Async("wikiTaskExecutor")
    public void collectFromLocalDump(Path dumpPath, String lang, String dumpDate) {
        String collectionId = CollectionIdGenerator.generateId(WIKI_COLLECTION_PREFIX);

        try {
            log.info("Wikipedia 수집 시작 - collectionId={}, lang={}, dumpDate={}", collectionId, lang, dumpDate);

            // 1) 데이터 수집: 덤프 파일을 파싱하여 NDJSON 샤드로 업로드하고 통계 정보 수집
            ShardStats stats = processWikipediaDump(dumpPath, lang, dumpDate, collectionId);

            // 2) 매니페스트 저장: 수집된 샤드들의 메타데이터를 포함한 매니페스트 파일을 MinIO에 저장
            String manifestUrl = storeManifest(collectionId, lang, dumpDate, stats);

            // 3) 수집 메타데이터 저장: 데이터베이스에 수집 작업의 메타데이터 정보 저장
            saveCollectionMetadata(collectionId, manifestUrl, lang, dumpDate);

            // 4) 성공 이벤트 발행: Kafka를 통해 수집 완료 이벤트를 다른 시스템에 알림
            eventAsyncInvoker.publishSuccess(
                    ContentSource.DOCS_WIKIPEDIA.name(),
                    KafkaTopics.RAW_DOCS_WIKIPEDIA,
                    collectionId,
                    manifestUrl,
                    stats.getPagesTotal()
            );

            log.info("✅ Wikipedia 수집 완료 - collectionId={}, pagesTotal={}, shardsTotal={}",
                    collectionId, stats.getPagesTotal(), stats.getShardsTotal());

        } catch (Exception e) {
            log.error("❌ Wikipedia 수집 실패 - collectionId={}, lang={}, dumpDate={}, dumpPath={}",
                    collectionId, lang, dumpDate, dumpPath, e);

            boolean retriable = RetryUtils.isRetriable(e);
            eventAsyncInvoker.publishError(
                    ContentSource.DOCS_WIKIPEDIA.name(),
                    KafkaTopics.RAW_DOCS_WIKIPEDIA_DLQ,
                    collectionId,
                    "",
                    "INTERNAL_SERVER_ERROR",
                    e.getMessage(),
                    retriable
            );
        }
    }

    /**
     * 덤프 파일을 열고 파싱/업로드를 수행하여 통계를 반환합니다.
     */
    private ShardStats processWikipediaDump(Path dumpPath, String lang, String dumpDate, String collectionId)
            throws IOException, XMLStreamException {

        try (InputStream inputStream = openMaybeCompressed(dumpPath)) {
            return parseAndUploadShards(inputStream, lang, dumpDate, collectionId);
        }
    }

    /**
     * bz2 확장자면 bzip2 스트림으로 감싸고, 아니면 버퍼링된 파일 스트림을 반환합니다.
     */
    private InputStream openMaybeCompressed(Path path) throws IOException {
        String fileName = WikiXmlUtil.getFileName(path);

        if (WikiXmlUtil.isCompressed(fileName)) {
            InputStream fileInputStream = new BufferedInputStream(Files.newInputStream(path));
            try {
                return new BZip2CompressorInputStream(fileInputStream, true);
            } catch (IOException e) {
                closeQuietly(fileInputStream);
                throw e;
            }
        } else {
            return new BufferedInputStream(Files.newInputStream(path));
        }
    }

    /**
     * 조용히 close 시도(실패 무시).
     */
    private void closeQuietly(InputStream stream) {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException ignore) {
            // 조용히 무시
        }
    }

    /**
     * XML을 파싱하여 샤드를 업로드하고, 최종 통계를 반환합니다.
     */
    private ShardStats parseAndUploadShards(InputStream inputStream, String lang, String dumpDate, String collectionId)
            throws XMLStreamException, IOException {

        String basePrefix = createBasePrefix(dumpDate, collectionId);
        WikiParsingContext context = WikiParsingContext.builder()
                .lang(lang)
                .dumpDate(dumpDate)
                .collectionId(collectionId)
                .pagesPerShard(pagesPerShard)
                .basePrefix(basePrefix)
                .build();

        XMLStreamReader xmlReader = null;
        try {
            XMLInputFactory factory = XMLInputFactory.newFactory();
            xmlReader = factory.createXMLStreamReader(inputStream, "UTF-8");
            xmlParser.parse(xmlReader, context);
        } finally {
            if (xmlReader != null) {
                try {
                    xmlReader.close();
                } catch (Exception ignore) {
                    //
                }
            }
        }

        return context.getStats();
    }

    /**
     * 매니페스트 JSON을 생성하여 MinIO에 저장하고 경로를 반환합니다.
     */
    private String storeManifest(String collectionId, String lang, String dumpDate, ShardStats stats) {
        try {
            WikiManifest manifest = WikiManifest.builder()
                    .collectionId(collectionId)
                    .lang(lang)
                    .dumpDate(dumpDate)
                    .pagesTotal(stats.getPagesTotal())
                    .shardsTotal(stats.getShardsTotal())
                    .bytesTotal(stats.getBytesTotal())
                    .shards(stats.getShardKeys())
                    .schemaVersion(SCHEMA_VERSION)
                    .createdAt(Instant.now().toString())
                    .build();

            String prefix = createBasePrefix(dumpDate, collectionId);
            String manifestKey = String.format("%s/manifest.json", prefix);

            byte[] manifestBytes = objectMapper.writeValueAsBytes(manifest);

            PutObjectArgs putArgs = PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_DOCS_WIKIPEDIA)
                    .object(manifestKey)
                    .stream(new ByteArrayInputStream(manifestBytes), manifestBytes.length, -1)
                    .contentType("application/json; charset=utf-8;")
                    .userMetadata(Map.of(
                            "collection-id", collectionId,
                            "schema-version", SCHEMA_VERSION
                    ))
                    .build();

            minioClient.putObject(putArgs);

            return String.format("minio://%s/%s", MinIOBuckets.RAW_DOCS_WIKIPEDIA, manifestKey);
        } catch (Exception e) {
            throw new MinioStorageException("매니페스트 저장 실패", e);
        }
    }

    /**
     * 수집 결과를 메타데이터 저장소에 저장합니다.
     */
    private void saveCollectionMetadata(String collectionId, String manifestUrl, String lang, String dumpDate) {
        ContentMetadata metadata = ContentMetadata.builder()
                .source(ContentSource.DOCS_WIKIPEDIA.getCode())
                .externalId(collectionId)
                .title(String.format("Wikipedia Dump %s %s", lang, dumpDate))
                .rawUri(manifestUrl)
                .collectionId(collectionId)
                .collectedAt(LocalDateTime.now())
                .build();

        contentMetadataRepository.saveAll(List.of(metadata));
    }

    /**
     * 업로드 경로의 날짜/수집ID 기반 prefix를 생성합니다.
     */
    private String createBasePrefix(String dumpDate, String collectionId) {
        LocalDate date = LocalDate.now();
        try {
            if (dumpDate != null && dumpDate.length() == 8) {
                date = LocalDate.parse(dumpDate, DUMP_DATE_FORMATTER);
            }
        } catch (Exception ignore) {
            log.warn("Invalid dump date format: {}, using today's date", dumpDate);
        }

        String formattedDate = date.format(PREFIX_DATE_FORMATTER);
        return String.format("%s/%s", formattedDate, collectionId);
    }
}


