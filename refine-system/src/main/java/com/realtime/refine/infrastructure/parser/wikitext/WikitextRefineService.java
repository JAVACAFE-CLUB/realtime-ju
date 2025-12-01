package com.realtime.refine.infrastructure.parser.wikitext;

import info.bliki.wiki.model.WikiModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Wikitext 마크업을 일반 텍스트로 변환하는 서비스
 * bliki-core 라이브러리를 사용하여 MediaWiki 문법을 정확하게 파싱
 */
@Service
@Slf4j
public class WikitextRefineService {

    /**
     * Wikitext를 일반 텍스트로 변환
     * bliki-core를 사용하여 MediaWiki 문법을 HTML로 변환한 후 텍스트만 추출
     * 
     * @param wikitext 원본 Wikitext 문자열
     * @return 변환된 일반 텍스트
     */
    public String convertToPlainText(String wikitext) {
        if (wikitext == null || wikitext.trim().isEmpty()) {
            return "";
        }

        long start = System.currentTimeMillis();
        log.debug("🔄 Wikitext 변환 시작 - inputLength={}", wikitext.length());

        try {
            // bliki-core를 사용하여 Wikitext를 HTML로 변환
            WikiModel wikiModel = new WikiModel("", "");
            
            // 템플릿과 카테고리 등을 무시하고 순수 텍스트만 추출
            String html = wikiModel.render(wikitext);
            
            // HTML 태그 제거 및 텍스트만 추출
            String plainText = html
                    .replaceAll("<[^>]+>", "")  // HTML 태그 제거
                    .replaceAll("&nbsp;", " ")   // HTML 엔티티 변환
                    .replaceAll("&amp;", "&")
                    .replaceAll("&lt;", "<")
                    .replaceAll("&gt;", ">")
                    .replaceAll("&quot;", "\"")
                    .replaceAll("&#39;", "'");

            // 공백 정리
            plainText = normalizeWhitespace(plainText);

            long elapsed = System.currentTimeMillis() - start;
            log.debug("✅ Wikitext 변환 완료 - elapsed={}ms, outputLength={}", elapsed, plainText.length());

            return plainText;

        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ Wikitext 변환 실패 - elapsed={}ms, error={}", elapsed, e.getMessage(), e);
            // 실패 시 기본 텍스트 반환 (마크업 제거만 시도)
            return removeBasicMarkup(wikitext);
        }
    }

    /**
     * 기본 마크업만 제거 (에러 발생 시 폴백)
     */
    private String removeBasicMarkup(String text) {
        if (text == null) {
            return "";
        }
        return text
                .replaceAll("\\[\\[", "")
                .replaceAll("\\]\\]", "")
                .replaceAll("'''", "")
                .replaceAll("''", "")
                .replaceAll("\\{\\{", "")
                .replaceAll("\\}\\}", "")
                .trim();
    }

    /**
     * 공백 정규화
     */
    private String normalizeWhitespace(String text) {
        if (text == null) {
            return "";
        }

        return text
                .replace("\r\n", "\n")
                .replace("\r", "\n")
                .replaceAll("\n{3,}", "\n\n")  // 3개 이상의 연속 줄바꿈을 2개로
                .replaceAll("[ \t]+", " ")      // 연속 공백을 하나로
                .trim();
    }
}

