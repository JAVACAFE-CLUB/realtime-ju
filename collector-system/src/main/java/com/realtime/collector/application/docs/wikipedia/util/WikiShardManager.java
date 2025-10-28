package com.realtime.collector.application.docs.wikipedia.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.exception.MinioStorageException;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Wikipedia 페이지를 NDJSON GZIP 샤드로 임시 파일에 기록하고 MinIO에 업로드하는 매니저입니다.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class WikiShardManager {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;

    /**
     * 지정된 인덱스로 새로운 임시 샤드 파일을 생성합니다.
     */
    public ShardWriter createShard(int shardIndex) throws IOException {
        return ShardWriter.openTemp(shardIndex, objectMapper);
    }

    /**
     * 샤드 파일을 MinIO로 업로드하고, 업로드된 바이트 수를 반환합니다.
     */
    public long uploadShard(ShardWriter shard, String shardKey, Map<String, String> metadata) {
        try {
            long size = Files.size(shard.getTempPath());

            try (InputStream inputStream = Files.newInputStream(shard.getTempPath())) {
                PutObjectArgs putArgs = PutObjectArgs.builder()
                        .bucket(MinIOBuckets.RAW_DOCS_WIKIPEDIA)
                        .object(shardKey)
                        .stream(inputStream, size, -1)
                        .contentType("application/x-ndjson")
                        .userMetadata(metadata)
                        .build();

                minioClient.putObject(putArgs);
            }

            cleanupTempFile(shard.getTempPath());
            return size;

        } catch (Exception e) {
            throw new MinioStorageException("샤드 업로드 실패", e);
        }
    }

    /**
     * 업로드 후 임시 파일 삭제를 시도합니다.
     */
    private void cleanupTempFile(Path tempPath) {
        try {
            Files.deleteIfExists(tempPath);
        } catch (IOException e) {
            log.warn("임시 파일 삭제 실패: {}", tempPath, e);
        }
    }

    /**
     * 임시 NDJSON GZIP 파일을 다루는 쓰기 도우미입니다.
     */
    public static class ShardWriter implements AutoCloseable {
        @Getter
        private final Path tempPath;
        private final GZIPOutputStream gzipOut;
        private final JsonGenerator jsonGen;
        @Getter
        private int pagesInShard = 0;

        private ShardWriter(Path tempPath, GZIPOutputStream gzipOut, JsonGenerator jsonGen) {
            this.tempPath = tempPath;
            this.gzipOut = gzipOut;
            this.jsonGen = jsonGen;
        }

        /**
         * 새로운 임시 파일을 열고 JSON 제너레이터를 준비합니다.
         */
        public static ShardWriter openTemp(int shardIndex, ObjectMapper objectMapper) throws IOException {
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
                try {
                    gz.close();
                } catch (IOException ignore) {
                }
                throw e;
            }
            return new ShardWriter(temp, gz, gen);
        }

        /**
         * 현재 샤드용 JSON 제너레이터를 반환합니다.
         */
        public JsonGenerator generator() {
            return jsonGen;
        }

        /**
         * 샤드 내 페이지 카운트를 증가시킵니다.
         */
        public void incrementPages() {
            pagesInShard++;
        }

        @Override
        /* JSON/GZIP 스트림을 정확히 종료합니다. */
        public void close() throws IOException {
            try {
                jsonGen.flush();
                jsonGen.close();
            } catch (Exception ignore) {
            }
            gzipOut.flush();
            gzipOut.finish();
            gzipOut.close();
        }
    }
}


