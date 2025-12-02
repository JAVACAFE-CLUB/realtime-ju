package com.realtime.serving.application.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import com.realtime.common.constants.ContentSource;
import com.realtime.serving.domain.ranking.RawKeyword;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch에서 키워드 집계
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KeywordAggregationService {

    private final ElasticsearchClient elasticsearchClient;

    private static final int TOP_KEYWORDS_SIZE = 100;

    /**
     * 소스별 상위 키워드 집계
     */
    public List<RawKeyword> aggregateKeywordsBySource(ContentSource source, LocalDateTime since) {
        try {
            SearchResponse<Void> response = elasticsearchClient.search(s -> s
                    .index("realtime-contents")
                    .size(0)  // 문서는 필요 없고 aggregation만 필요
                    .query(q -> q
                            .bool(b -> b
                                    .filter(f -> f
                                            .term(t -> t
                                                    .field("sourceType")
                                                    .value(source.name())
                                            )
                                    )
                                    .filter(f -> f
                                            .range(r -> r
                                                    .field("indexedAt")
                                                    .gte(JsonData.of(since.toString()))
                                            )
                                    )
                            )
                    )
                    .aggregations("top_keywords", Aggregation.of(a -> a
                            .terms(t -> t
                                    .field("keywords")
                                    .size(TOP_KEYWORDS_SIZE)
                            )
                    )),
                    Void.class
            );

            // Aggregation 결과 파싱
            StringTermsAggregate topKeywords = response.aggregations()
                    .get("top_keywords")
                    .sterms();

            List<RawKeyword> results = new ArrayList<>();
            for (StringTermsBucket bucket : topKeywords.buckets().array()) {
                results.add(RawKeyword.builder()
                        .keyword(bucket.key().stringValue())
                        .docCount(bucket.docCount())
                        .source(source)
                        .indexedAt(LocalDateTime.now())  // 집계 시점
                        .build());
            }

            log.info("Aggregated {} keywords from {} (since: {})", results.size(), source, since);
            return results;

        } catch (Exception e) {
            log.error("Failed to aggregate keywords from {} (since: {})", source, since, e);
            return List.of();
        }
    }

    /**
     * 모든 소스의 키워드 집계
     */
    public Map<ContentSource, List<RawKeyword>> aggregateAllKeywords(LocalDateTime since) {
        return Map.of(
                ContentSource.NEWS_YNA, aggregateKeywordsBySource(ContentSource.NEWS_YNA, since),
                ContentSource.SNS_YOUTUBE, aggregateKeywordsBySource(ContentSource.SNS_YOUTUBE, since),
                ContentSource.DOCS_WIKIPEDIA, aggregateKeywordsBySource(ContentSource.DOCS_WIKIPEDIA, since)
        );
    }
}
