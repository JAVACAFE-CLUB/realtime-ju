package com.realtime.serving.domain.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

/**
 * 키워드 관련 소스 링크 정보
 */
@Getter
@Builder
public class SourceLink {
    private String source;      // "yna", "wikipedia", "youtube"
    private String url;         // 원본 링크
    private String title;       // 문서 제목

    @JsonCreator
    public SourceLink(
            @JsonProperty("source") String source,
            @JsonProperty("url") String url,
            @JsonProperty("title") String title) {
        this.source = source;
        this.url = url;
        this.title = title;
    }

    public static SourceLink of(String source, String url, String title) {
        return SourceLink.builder()
                .source(source)
                .url(url)
                .title(title)
                .build();
    }
}
