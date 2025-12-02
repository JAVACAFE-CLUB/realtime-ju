package com.realtime.index.domain.keyword;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExtractedKeyword {

    private String keyword;
    private Long frequency;
    private Double score;

    public static ExtractedKeyword of(String keyword, Long frequency) {
        return ExtractedKeyword.builder()
                .keyword(keyword)
                .frequency(frequency)
                .score(frequency.doubleValue())
                .build();
    }
}
