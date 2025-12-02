package com.realtime.serving.infrastructure.persistence.elasticsearch;

import com.realtime.common.constants.ContentSource;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Elasticsearch realtime-contents 인덱스의 문서 모델
 */
@Document(indexName = "realtime-contents")
@Getter
@Setter
public class IndexedDocument {

    @Id
    private String id;

    @Field(type = FieldType.Text)
    private String title;

    @Field(type = FieldType.Text)
    private String content;

    @Field(type = FieldType.Keyword)
    private List<String> keywords;

    @Field(type = FieldType.Keyword, name = "sourceType")
    private ContentSource source;

    @Field(type = FieldType.Date)
    private LocalDateTime indexedAt;
}
