package com.realtime.refine.infrastructure.parser.jsoup;

import com.realtime.refine.infrastructure.parser.tika.TikaResult;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class JsoupRefineService {

    /**
     * InputStream에서 직접 HTML을 읽어서 파싱 (Tika 없이)
     */
    public RefinedText parseHtml(InputStream inputStream, String charset) {
        long start = System.currentTimeMillis();
        try {
            // [1단계] InputStream에서 HTML 파싱
            log.debug("🔄 HTML 파싱 시작 - charset={}", charset);
            Document doc = Jsoup.parse(inputStream, charset, "");
            
            int docLength = doc.html().length();
            log.info("🔄 HTML 파싱 완료 - docLength={}, charset={}", docLength, charset);

            // [2단계] 제목 추출 (먼저)
            String title = firstText(doc, "h1.tit01, strong.tit-news, h1.tit, h1.news-title, #articleTitle, .tit-article");
            log.debug("📝 제목 추출 - titleLength={}, title={}", 
                    title != null ? title.length() : 0,
                    title != null && !title.isEmpty() ? title.substring(0, Math.min(50, title.length())) : "EMPTY");

            // [3단계] 본문 영역 선택 - 연합뉴스의 <div class="story-news"> 정확히 타겟
            Elements bodyElements = doc.select("div.story-news, div.article-body, #articleBody, .news-contents");
            log.info("🔍 본문 영역 매칭 - count={}", bodyElements.size());
            
            String body = "";
            if (!bodyElements.isEmpty()) {
                Element bodyElement = bodyElements.first();
                
                int initialLength = bodyElement.text().length();
                log.info("📍 선택된 본문 영역 - <{}{}{}>, 초기 텍스트 길이={}", 
                         bodyElement.tagName(),
                         bodyElement.id().isEmpty() ? "" : " id=\"" + bodyElement.id() + "\"",
                         bodyElement.classNames().isEmpty() ? "" : " class=\"" + String.join(" ", bodyElement.classNames()) + "\"",
                         initialLength);
                
                if (initialLength == 0) {
                    log.warn("⚠️ 선택된 본문 영역이 비어있음! HTML 내용: {}", bodyElement.html().substring(0, Math.min(200, bodyElement.html().length())));
                }
                
                // [4단계] 본문 안에서만 불필요한 요소 제거
                int removedCount = bodyElement.select(
                        "script, style, " +                          // 스크립트/스타일
                        "aside, " +                                   // 광고 (aside 태그)
                        ".ad, .banner, .ads-article01, " +           // 광고 클래스
                        ".comp-box, " +                              // 이미지 박스
                        ".keyword-zone, " +                          // 해시태그
                        ".empathy-zone, " +                          // 좋아요/슬퍼요 버튼
                        ".comment-zone, " +                          // 댓글
                        ".txt-copyright, " +                         // 저작권 안내
                        ".option-zone, " +                           // 공유/북마크 버튼
                        ".related-zone, " +                          // 관련 뉴스
                        ".label-box03, " +                           // 이미지 확대 라벨
                        ".story-summary, " +                         // 세 줄 요약
                        ".tlp-summary01, " +                         // 세 줄 요약 툴팁
                        ".tooltip-type01, " +                        // 툴팁 전체
                        ".writer-zone01, " +                         // 기자 정보
                        ".tit-sub, " +                               // 부제목
                        "figcaption, " +                             // 이미지 캡션
                        "button, " +                                 // 모든 버튼
                        "nav, header, footer"                        // 네비게이션/헤더/푸터
                ).size();
                bodyElement.select(
                        "script, style, " +
                        "aside, " +
                        ".ad, .banner, .ads-article01, " +
                        ".comp-box, " +
                        ".keyword-zone, " +
                        ".empathy-zone, " +
                        ".comment-zone, " +
                        ".txt-copyright, " +
                        ".option-zone, " +
                        ".related-zone, " +
                        ".label-box03, " +
                        ".story-summary, " +
                        ".tlp-summary01, " +
                        ".tooltip-type01, " +
                        ".writer-zone01, " +
                        ".tit-sub, " +
                        "figcaption, " +
                        "button, " +
                        "nav, header, footer"
                ).remove();
                
                int afterRemoveLength = bodyElement.text().length();
                log.info("🧹 본문 내 불필요 요소 제거 - count={}, 제거 전={}, 제거 후={}", 
                         removedCount, initialLength, afterRemoveLength);
                
                // [5단계] 정제된 본문 텍스트 추출
                body = bodyElement.text();
                log.info("📝 본문 텍스트 추출 완료 - bodyLength={}", body.length());
            } else {
                log.warn("⚠️ 본문 영역을 찾을 수 없음!");
            }
            
            log.info("📝 본문 추출 완료 - bodyLength={}, bodyPreview={}", 
                    body != null ? body.length() : 0,
                    body != null && body.length() > 100 ? body.substring(0, 100) + "..." : body);

            // [6단계] 공백/불용 문구 정규화
            String normalized = normalizeWhitespace(body);
            log.info("📝 정규화 완료 - normalizedLength={}", normalized != null ? normalized.length() : 0);

            long elapsed = System.currentTimeMillis() - start;
            log.info("✅ HTML 파싱 완료 - elapsed={}ms, title={}, contentLength={}", 
                    elapsed, 
                    title != null ? title.substring(0, Math.min(30, title.length())) + "..." : "null",
                    normalized != null ? normalized.length() : 0);

            return new RefinedText(title, normalized);
        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ HTML 파싱 실패 - elapsed={}ms, error={}", elapsed, e.getMessage(), e);
            throw new RuntimeException("HTML parse failed", e);
        }
    }

    public RefinedText refine(TikaResult tika) {
        long start = System.currentTimeMillis();
        try {
            // [1단계] HTML 문자열 파싱
            String htmlOrText = tika.getContent();
            log.info("🔄 Jsoup 정제 시작 - inputLength={}, inputPreview={}", 
                    htmlOrText != null ? htmlOrText.length() : 0,
                    htmlOrText != null && htmlOrText.length() > 100 ? htmlOrText.substring(0, 100) + "..." : htmlOrText);
            
            if (htmlOrText == null || htmlOrText.trim().isEmpty()) {
                log.warn("⚠️ Tika 결과가 비어있음!");
                return new RefinedText("", "");
            }
            
            Document doc = Jsoup.parse(htmlOrText);

            // [2단계] 제목 추출 (먼저)
            String title = firstText(doc, "h1.tit01, strong.tit-news, h1.tit, h1.news-title, #articleTitle, .tit-article");
            log.debug("📝 제목 추출 - titleLength={}, title={}", 
                    title != null ? title.length() : 0,
                    title != null && !title.isEmpty() ? title.substring(0, Math.min(50, title.length())) : "EMPTY");

            // [3단계] 본문 영역 선택 - 연합뉴스의 <div class="story-news"> 정확히 타겟
            Elements bodyElements = doc.select("div.story-news, div.article-body, #articleBody, .news-contents");
            log.info("🔍 본문 영역 매칭 - count={}", bodyElements.size());
            
            String body = "";
            if (!bodyElements.isEmpty()) {
                Element bodyElement = bodyElements.first();
                
                int initialLength = bodyElement.text().length();
                log.info("📍 선택된 본문 영역 - <{}{}{}>, 초기 텍스트 길이={}", 
                         bodyElement.tagName(),
                         bodyElement.id().isEmpty() ? "" : " id=\"" + bodyElement.id() + "\"",
                         bodyElement.classNames().isEmpty() ? "" : " class=\"" + String.join(" ", bodyElement.classNames()) + "\"",
                         initialLength);
                
                if (initialLength == 0) {
                    log.warn("⚠️ 선택된 본문 영역이 비어있음! HTML 내용: {}", bodyElement.html().substring(0, Math.min(200, bodyElement.html().length())));
                }
                
                // [4단계] 본문 안에서만 불필요한 요소 제거
                int removedCount = bodyElement.select(
                        "script, style, " +
                        "aside, " +
                        ".ad, .banner, .ads-article01, " +
                        ".comp-box, " +
                        ".keyword-zone, " +
                        ".empathy-zone, " +
                        ".comment-zone, " +
                        ".txt-copyright, " +
                        ".option-zone, " +
                        ".related-zone, " +
                        ".label-box03, " +
                        ".story-summary, " +
                        ".tlp-summary01, " +
                        ".tooltip-type01, " +
                        ".writer-zone01, " +
                        ".tit-sub, " +
                        "figcaption, " +
                        "button, " +
                        "nav, header, footer"
                ).size();
                bodyElement.select(
                        "script, style, " +
                        "aside, " +
                        ".ad, .banner, .ads-article01, " +
                        ".comp-box, " +
                        ".keyword-zone, " +
                        ".empathy-zone, " +
                        ".comment-zone, " +
                        ".txt-copyright, " +
                        ".option-zone, " +
                        ".related-zone, " +
                        ".label-box03, " +
                        ".story-summary, " +
                        ".tlp-summary01, " +
                        ".tooltip-type01, " +
                        ".writer-zone01, " +
                        ".tit-sub, " +
                        "figcaption, " +
                        "button, " +
                        "nav, header, footer"
                ).remove();
                
                int afterRemoveLength = bodyElement.text().length();
                log.info("🧹 본문 내 불필요 요소 제거 - count={}, 제거 전={}, 제거 후={}", 
                         removedCount, initialLength, afterRemoveLength);
                
                // [5단계] 정제된 본문 텍스트 추출
                body = bodyElement.text();
                log.info("📝 본문 텍스트 추출 완료 - bodyLength={}", body.length());
                
                if (body.length() > 0) {
                    log.info("📄 본문 미리보기 (앞 300자):\n{}", 
                             body.length() > 300 ? body.substring(0, 300) + "..." : body);
                } else {
                    log.warn("⚠️ 본문 텍스트가 비어있음!");
                }
            } else {
                log.warn("⚠️ 본문 영역을 찾을 수 없음!");
            }
            
            log.info("📝 본문 추출 완료 - bodyLength={}, bodyPreview={}", 
                    body != null ? body.length() : 0,
                    body != null && body.length() > 100 ? body.substring(0, 100) + "..." : body);

            // [6단계] 공백/불용 문구 정규화
            String normalized = normalizeWhitespace(body);
            log.info("📝 정규화 완료 - normalizedLength={}", normalized != null ? normalized.length() : 0);

            long elapsed = System.currentTimeMillis() - start;
            log.debug("✅ Jsoup 정제 완료 - elapsed={}ms, title={}, contentLength={}", 
                    elapsed, 
                    title != null ? title.substring(0, Math.min(30, title.length())) + "..." : "null",
                    normalized != null ? normalized.length() : 0);

            return new RefinedText(title, normalized);
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ Jsoup 정제 실패 - elapsed={}ms, error={}", elapsed, e.getMessage(), e);
            throw new RuntimeException("Jsoup refine failed", e);
        }
    }

    private String firstText(Document doc, String selector) {
        Elements els = doc.select(selector);
        log.debug("🔍 셀렉터 매칭 - selector={}, matchCount={}", selector, els.size());
        if (els.isEmpty()) {
            log.warn("⚠️ 셀렉터 매칭 실패 - selector={}", selector);
            return "";
        }
        Element e = els.first();
        String text = e != null ? e.text() : "";
        log.debug("✅ 셀렉터 매칭 성공 - selector={}, textLength={}", selector, text.length());
        return text;
    }

    private String normalizeWhitespace(String text) {
        if (text == null) return "";
        
        log.debug("🔧 정규화 시작 - inputLength={}", text.length());
        
        // [1단계] 기본 공백 정리
        String normalized = text
                .replace("\r\n", "\n")
                .replace("\r", "\n")
                .replaceAll("&nbsp;", " ")
                .replaceAll("\n+", "\n")
                .replaceAll("[ \t]+", " ")
                .trim();
        
        log.debug("🔧 기본 공백 정리 완료 - length={}", normalized.length());
        
        // [2단계] 이메일 주소가 포함된 줄만 제거 (전체 줄이 이메일인 경우만)
        String beforeEmail = normalized;
        normalized = normalized.replaceAll("(?m)^\\s*[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}\\s*$", "");
        if (!beforeEmail.equals(normalized)) {
            log.debug("🔧 이메일 제거 - before={}, after={}", beforeEmail.length(), normalized.length());
        }
        
        // [3단계] 저작권 안내 제거
        String beforeCopyright = normalized;
        normalized = normalized.replaceAll("(?m)^\\s*(저작권자|무단전재|무단 전재|재배포 금지).*$", "");
        if (!beforeCopyright.equals(normalized)) {
            log.debug("🔧 저작권 제거 - before={}, after={}", beforeCopyright.length(), normalized.length());
        }
        
        // [4단계] 연속된 공백/줄바꿈 정리
        normalized = normalized.replaceAll("\n{3,}", "\n\n").trim();
        
        log.debug("🔧 정규화 완료 - outputLength={}", normalized.length());
        
        return normalized;
    }

    public record RefinedText(String title, String content) {}
}


