package com.realtime.collector.application.docs.wikipedia.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Wikipedia 페이지의 핵심 메타/본문 데이터를 담는 전송 객체.
 * 파싱 단계에서 필요한 필드만 얇게 유지합니다.
 */
@Getter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class WikiPage {
    
    /** wiki page의 내부 식별자 */
    private String pageId;
    private String title;
    /** 네임스페이스 번호(ns). 문서=0 등 */
    private Integer ns;
    /** 리다이렉트 대상 제목(있을 경우) */
    private String redirectTitle;
    /** 최신 리비전 ID */
    private String revisionId;
    /** 최신 리비전 타임스탬프 */
    private String timestamp;
    /** 기여자(사용자명 등) */
    private String contributor;
    /** 문서 본문(wikitext/raw) */
    private String text;
}
