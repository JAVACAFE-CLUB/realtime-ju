package com.realtime.index.domain.document;

import com.realtime.common.constants.ContentSource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.Setting;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Document(indexName = "realtime-contents")
@Setting(shards = 3, replicas = 1, refreshInterval = "5s")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexedDocument {

    @Id
    private String id;

    @Field(type = FieldType.Keyword)
    private String documentId;  // MongoDB ObjectId 또는 원본 ID

    @Field(type = FieldType.Keyword)
    private ContentSource sourceType;

    @Field(type = FieldType.Text, analyzer = "standard")
    private String title;

    @Field(type = FieldType.Text, analyzer = "standard")
    private String content;

    @Field(type = FieldType.Keyword)
    private List<String> keywords;  // 추출된 키워드

    @Field(type = FieldType.Keyword)
    private String author;

    @Field(type = FieldType.Keyword)
    private String category;

    @Field(type = FieldType.Date)
    private LocalDateTime publishedAt;

    @Field(type = FieldType.Date)
    private LocalDateTime indexedAt;

    @Field(type = FieldType.Long)
    private Long popularity;  // 조회수, 좋아요 등

    @Field(type = FieldType.Object, enabled = false)
    private Map<String, Object> metadata;  // 동적 메타데이터
}
