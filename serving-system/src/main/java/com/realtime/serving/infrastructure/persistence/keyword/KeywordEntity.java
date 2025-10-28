package com.realtime.serving.infrastructure.persistence.keyword;

import com.realtime.serving.domain.keyword.entity.TrendStatus;
import com.realtime.serving.infrastructure.persistence.shared.BaseJpaEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "keywords",
        indexes = {
                @Index(name = "idx_keyword_ranking", columnList = "ranking"),
                @Index(name = "idx_keyword_updated_at", columnList = "updated_at"),
                @Index(name = "idx_keyword_keyword", columnList = "keyword")
        },
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_keyword_ranking", columnNames = "ranking")
        })
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class KeywordEntity extends BaseJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "keyword", nullable = false, length = 100)
    private String keyword;

    @Column(name = "ranking", nullable = false, unique = true)
    private Integer ranking;

    @Enumerated(EnumType.STRING)
    @Column(name = "trend_status", nullable = false)
    private TrendStatus trendStatus;

    @Column(name = "search_count", nullable = false)
    @Builder.Default
    private Long searchCount = 0L;
}
