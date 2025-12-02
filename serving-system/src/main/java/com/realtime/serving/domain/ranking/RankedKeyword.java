package com.realtime.serving.domain.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * 가중치가 적용된 최종 랭킹 키워드
 */
@Getter
@Builder
public class RankedKeyword {
    private String keyword;
    private double score;
    private int rank;
    private List<SourceLink> sources;

    @JsonCreator
    public RankedKeyword(
            @JsonProperty("keyword") String keyword,
            @JsonProperty("score") double score,
            @JsonProperty("rank") int rank,
            @JsonProperty("sources") List<SourceLink> sources) {
        this.keyword = keyword;
        this.score = score;
        this.rank = rank;
        this.sources = sources;
    }

    public static RankedKeyword of(String keyword, double score, int rank) {
        return RankedKeyword.builder()
                .keyword(keyword)
                .score(score)
                .rank(rank)
                .build();
    }

    public static RankedKeyword of(String keyword, double score, int rank, List<SourceLink> sources) {
        return RankedKeyword.builder()
                .keyword(keyword)
                .score(score)
                .rank(rank)
                .sources(sources)
                .build();
    }
}
