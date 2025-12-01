package com.realtime.refine.infrastructure.parser.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class JsonRefineService {

    private final ObjectMapper objectMapper;

    /**
     * InputStream에서 JSON을 읽어서 YouTubeVideo 객체로 변환
     */
    public <T> T parseJson(InputStream inputStream, Class<T> clazz) {
        long start = System.currentTimeMillis();
        try {
            log.debug("🔄 JSON 파싱 시작 - targetClass={}", clazz.getSimpleName());

            T result = objectMapper.readValue(inputStream, clazz);

            long elapsed = System.currentTimeMillis() - start;
            log.info("✅ JSON 파싱 완료 - elapsed={}ms, targetClass={}", elapsed, clazz.getSimpleName());

            return result;
        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ JSON 파싱 실패 - elapsed={}ms, targetClass={}, error={}",
                    elapsed, clazz.getSimpleName(), e.getMessage(), e);
            throw new RuntimeException("JSON parse failed", e);
        }
    }

    /**
     * YouTube 텍스트 필드 정규화 (공백, 줄바꿈 정리)
     */
    public String normalizeText(String text) {
        if (text == null) {
            return "";
        }

        log.debug("🔧 텍스트 정규화 시작 - inputLength={}", text.length());

        // [1단계] 기본 공백 정리
        String normalized = text
                .replace("\r\n", "\n")
                .replace("\r", "\n")
                .replaceAll("\n{3,}", "\n\n")  // 3개 이상의 연속 줄바꿈을 2개로
                .replaceAll("[ \t]+", " ")      // 연속 공백을 하나로
                .trim();

        log.debug("🔧 텍스트 정규화 완료 - outputLength={}", normalized.length());

        return normalized;
    }

    /**
     * YouTube 컨텐츠 결과 레코드
     */
    public record RefinedYouTubeContent(
            String title,
            String description,
            String tags,
            String channelId,
            String channelTitle,
            String categoryId,
            String publishedAt,
            String thumbnailUrl,
            String language
    ) {}
}
