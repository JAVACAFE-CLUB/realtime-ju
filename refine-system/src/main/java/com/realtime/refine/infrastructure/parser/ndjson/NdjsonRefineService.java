package com.realtime.refine.infrastructure.parser.ndjson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.refine.application.docs.wikipedia.dto.WikiPage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * NDJSON (Newline Delimited JSON) 스트리밍 파서 서비스
 * GZIP 압축된 NDJSON 파일을 스트리밍 방식으로 파싱
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NdjsonRefineService {

    private final ObjectMapper objectMapper;

    /**
     * GZIP 압축된 NDJSON 스트림을 파싱하여 각 페이지를 콜백으로 전달
     * 
     * @param inputStream GZIP 압축된 NDJSON 스트림
     * @param pageConsumer 각 페이지를 처리하는 콜백
     * @return 처리된 페이지 수
     */
    public int parseShard(InputStream inputStream, Consumer<WikiPage> pageConsumer) {
        long start = System.currentTimeMillis();
        int pageCount = 0;

        try {
            log.debug("🔄 NDJSON 스트리밍 파싱 시작");

            // GZIP 압축 해제
            try (GZIPInputStream gzipIn = new GZIPInputStream(inputStream);
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(gzipIn, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue; // 빈 라인 스킵
                    }

                    try {
                        // 각 라인을 WikiPage 객체로 파싱
                        WikiPage page = objectMapper.readValue(line, WikiPage.class);
                        pageConsumer.accept(page);
                        pageCount++;

                        if (pageCount % 100 == 0) {
                            log.debug("📄 페이지 파싱 진행 - count={}", pageCount);
                        }

                    } catch (Exception e) {
                        log.warn("⚠️ 페이지 파싱 실패 - line={}, error={}", 
                                line.substring(0, Math.min(100, line.length())), e.getMessage());
                        // 개별 페이지 파싱 실패는 계속 진행
                    }
                }
            }

            long elapsed = System.currentTimeMillis() - start;
            log.info("✅ NDJSON 스트리밍 파싱 완료 - elapsed={}ms, pageCount={}", elapsed, pageCount);

            return pageCount;

        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ NDJSON 스트리밍 파싱 실패 - elapsed={}ms, pageCount={}, error={}", 
                    elapsed, pageCount, e.getMessage(), e);
            throw new RuntimeException("NDJSON parse failed", e);
        }
    }
}

