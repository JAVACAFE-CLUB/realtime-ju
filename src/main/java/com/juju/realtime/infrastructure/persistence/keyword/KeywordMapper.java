package com.juju.realtime.infrastructure.persistence.keyword;

import com.juju.realtime.domain.keyword.entity.Keyword;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class KeywordMapper {

    public KeywordEntity toEntity(Keyword domain) {
        return KeywordEntity.builder()
                .id(domain.getId())
                .keyword(domain.getKeyword())
                .ranking(domain.getRanking())
                .trendStatus(domain.getTrendStatus())
                .searchCount(domain.getSearchCount())
                .build();
    }

    public Keyword toDomain(KeywordEntity entity) {

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

    public List<Keyword> toDomainList(List<KeywordEntity> entities) {
        return entities.stream()
                .map(this::toDomain)
                .collect(Collectors.toList());
    }
}
