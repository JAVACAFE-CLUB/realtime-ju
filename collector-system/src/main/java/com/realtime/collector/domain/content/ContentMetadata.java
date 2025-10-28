package com.realtime.collector.domain.content;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "content_metadata")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ContentMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source", nullable = false, length = 20)
    private String source; // YOUTUBE/WIKI/NEWS

    @Column(name = "external_id", nullable = false, length = 128)
    private String externalId;

    @Column(name = "title", nullable = false, length = 500)
    private String title;

    @Column(name = "raw_uri", nullable = false, length = 1024)
    private String rawUri; // minio://bucket/path

    @Column(name = "refined_id", length = 24)
    private String refinedId; // Mongo ObjectId 문자열 등

    @Column(name = "collection_id", length = 40)
    private String collectionId;

    @Column(name = "collected_at", nullable = false)
    private LocalDateTime collectedAt;
}


