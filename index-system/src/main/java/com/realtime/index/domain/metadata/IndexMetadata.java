package com.realtime.index.domain.metadata;

import com.realtime.common.constants.ContentSource;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "index_metadata",
        indexes = {
                @Index(name = "idx_refined_id", columnList = "refinedId", unique = true),
                @Index(name = "idx_document_id", columnList = "documentId"),
                @Index(name = "idx_source_type", columnList = "sourceType"),
                @Index(name = "idx_indexed_at", columnList = "indexedAt")
        })
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String refinedId;  // MongoDB ObjectId from RefineMessage

    @Column(nullable = false)
    private String documentId;  // Elasticsearch document ID

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private ContentSource sourceType;

    @Column(nullable = false)
    private LocalDateTime indexedAt;

    @Column
    private String errorMessage;  // 색인 실패 시 에러 메시지

    @Builder.Default
    @Column(nullable = false)
    private Boolean isSuccess = true;

    public void markAsFailed(String errorMessage) {
        this.isSuccess = false;
        this.errorMessage = errorMessage;
    }
}
