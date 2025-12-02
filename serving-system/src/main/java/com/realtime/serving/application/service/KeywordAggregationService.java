package com.realtime.serving.application.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TopHitsAggregate;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
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
     * 소스별 상위 키워드 집계 (대표 문서 링크 포함)
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
                            .aggregations("top_doc", Aggregation.of(sub -> sub
                                    .topHits(th -> th
                                            .size(1)
                                            .source(src -> src
                                                    .filter(f -> f
                                                            .includes("title", "metadata")
                                                    )
                                            )
                                            .sort(so -> so
                                                    .field(fs -> fs
                                                            .field("popularity")
                                                            .order(SortOrder.Desc)
                                                    )
                                            )
                                    )
                            ))
                    )),
                    Void.class
            );

            // Aggregation 결과 파싱
            StringTermsAggregate topKeywords = response.aggregations()
                    .get("top_keywords")
                    .sterms();

            List<RawKeyword> results = new ArrayList<>();
            for (StringTermsBucket bucket : topKeywords.buckets().array()) {
                String documentUrl = null;
                String documentTitle = null;

                // top_doc에서 대표 문서 정보 추출
                TopHitsAggregate topDoc = bucket.aggregations().get("top_doc").topHits();
                if (!topDoc.hits().hits().isEmpty()) {
                    Hit<JsonData> hit = topDoc.hits().hits().get(0);
                    JsonData sourceData = hit.source();
                    if (sourceData != null) {
                        try {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> sourceMap = sourceData.to(Map.class);
                            documentTitle = sourceMap.get("title") != null ? sourceMap.get("title").toString() : null;
                            documentUrl = extractUrlFromMetadata(sourceMap, source);
                        } catch (Exception e) {
                            log.warn("Failed to parse source data for keyword {}: {}",
                                    bucket.key().stringValue(), e.getMessage());
                        }
                    }
                }

                results.add(RawKeyword.builder()
                        .keyword(bucket.key().stringValue())
                        .docCount(bucket.docCount())
                        .source(source)
                        .indexedAt(LocalDateTime.now())
                        .documentUrl(documentUrl)
                        .documentTitle(documentTitle)
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
     * metadata에서 URL 추출
     */
    @SuppressWarnings("unchecked")
    private String extractUrlFromMetadata(Map<String, Object> sourceMap, ContentSource source) {
        Object metadataObj = sourceMap.get("metadata");
        if (metadataObj == null) {
            return null;
        }

        Map<String, Object> metadata = (Map<String, Object>) metadataObj;

        switch (source) {
            case NEWS_YNA:
                // contentId로 연합뉴스 원본 URL 생성
                if (metadata.get("contentId") != null) {
                    String contentId = metadata.get("contentId").toString();
                    return "https://www.yna.co.kr/view/" + contentId;
                }
                return null;

            case DOCS_WIKIPEDIA:
                if (metadata.get("pageId") != null) {
                    String pageId = metadata.get("pageId").toString();
                    return "https://ko.wikipedia.org/?curid=" + pageId;
                }
                return null;

            case SNS_YOUTUBE:
                // videoId로 YouTube URL 생성
                if (metadata.get("videoId") != null) {
                    return "https://www.youtube.com/watch?v=" + metadata.get("videoId").toString();
                }
                return null;

            default:
                return null;
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
