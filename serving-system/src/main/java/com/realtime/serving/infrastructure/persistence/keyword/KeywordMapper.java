package com.realtime.serving.infrastructure.persistence.keyword;

import com.realtime.serving.domain.keyword.entity.Keyword;
import java.util.List;

public class KeywordMapper {

    private KeywordMapper() {
        // 유틸리티 클래스이므로 인스턴스화를 방지
    }

    public static KeywordEntity toEntity(Keyword domain) {
        return KeywordEntity.builder()
                .id(domain.getId())
                .keyword(domain.getKeyword())
                .ranking(domain.getRanking())
                .trendStatus(domain.getTrendStatus())
                .searchCount(domain.getSearchCount())
                .build();
    }

    public static Keyword toDomain(KeywordEntity entity) {
        return Keyword.builder()
                .id(entity.getId())
                .keyword(entity.getKeyword())
                .ranking(entity.getRanking())
                .trendStatus(entity.getTrendStatus())
                .searchCount(entity.getSearchCount())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .build();
    }

    public static List<Keyword> toDomainList(List<KeywordEntity> entities) {
        return entities.stream()
                .map(KeywordMapper::toDomain)
                .toList();
    }
}
