package com.realtime.common.config;

import com.realtime.common.constants.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 토픽 자동 생성 설정
 * - 스프링이 {@link org.apache.kafka.clients.admin.NewTopic} 빈을 감지하면 {@link org.springframework.kafka.core.KafkaAdmin}
 *   을 통해 애플리케이션 기동 시 토픽을 보장(없으면 생성, 있으면 스킵)합니다.
 */
@Configuration
@Slf4j
public class KafkaTopicConfig {

    // 파티션 설정 상수
    private static final int RAW_TOPIC_PARTITIONS = 6;
    private static final int REFINED_TOPIC_PARTITIONS = 6;

    @Value("${kafka.topic.partitions:3}")
    private int defaultPartitions;

    @Value("${kafka.topic.replication-factor:1}")
    private short defaultReplicationFactor;

    @Value("${kafka.topic.retention.raw-data-hours:168}") // 7일
    private String rawDataRetentionHours;

    @Value("${kafka.topic.retention.refined-data-hours:336}") // 14일
    private String refinedDataRetentionHours;

    // Raw Data Topics (원본 데이터)
    @Bean
    public NewTopic rawWikipediaTopic() {
        String retentionMs = hoursToMs(rawDataRetentionHours);
        NewTopic topic = TopicBuilder.name(KafkaTopics.RAW_DOCS_WIKIPEDIA)
                .partitions(RAW_TOPIC_PARTITIONS)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", retentionMs)
                .config("segment.ms", "86400000") // 1일
                .config("compression.type", "zstd")
                .config("max.message.bytes", "10485760") // 10MB
                .config("min.insync.replicas", "1")
                .build();
        log.info("✅ Ensuring topic: name={}, partitions={}, replicas={}, retentionMs={} (raw)",
                KafkaTopics.RAW_DOCS_WIKIPEDIA, RAW_TOPIC_PARTITIONS, defaultReplicationFactor, retentionMs);
        return topic;
    }

    @Bean
    public NewTopic rawNewsYnaTopic() {
        String retentionMs = hoursToMs(rawDataRetentionHours);
        NewTopic topic = TopicBuilder.name(KafkaTopics.RAW_NEWS_YNA)
                .partitions(RAW_TOPIC_PARTITIONS)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", retentionMs)
                .config("segment.ms", "86400000") // 1일
                .config("compression.type", "zstd")
                .build();
        log.info("✅ Ensuring topic: name={}, partitions={}, replicas={}, retentionMs={} (raw)",
                KafkaTopics.RAW_NEWS_YNA, RAW_TOPIC_PARTITIONS, defaultReplicationFactor, retentionMs);
        return topic;
    }

    @Bean
    public NewTopic rawSnsYouTubeTopic() {
        String retentionMs = hoursToMs(rawDataRetentionHours);
        NewTopic topic = TopicBuilder.name(KafkaTopics.RAW_SNS_YOUTUBE)
                .partitions(RAW_TOPIC_PARTITIONS)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", retentionMs)
                .config("segment.ms", "3600000") // 1시간
                .config("compression.type", "zstd")
                .config("max.message.bytes", "10485760") // 10MB (동영상 메타데이터 대비)
                .build();
        log.info("✅ Ensuring topic: name={}, partitions={}, replicas={}, retentionMs={} (raw)",
                KafkaTopics.RAW_SNS_YOUTUBE, RAW_TOPIC_PARTITIONS, defaultReplicationFactor, retentionMs);
        return topic;
    }

    // Refined Data Topics (정제된 데이터)
    @Bean
    public NewTopic refinedArticlesTopic() {
        String retentionMs = hoursToMs(refinedDataRetentionHours);
        NewTopic topic = TopicBuilder.name(KafkaTopics.REFINED_DOCS_WIKIPEDIA)
                .partitions(REFINED_TOPIC_PARTITIONS)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", retentionMs)
                .config("compression.type", "zstd")
                .build();
        log.info("✅ Ensuring topic: name={}, partitions={}, replicas={}, retentionMs={} (refined)",
                KafkaTopics.REFINED_DOCS_WIKIPEDIA, REFINED_TOPIC_PARTITIONS, defaultReplicationFactor, retentionMs);
        return topic;
    }

    @Bean
    public NewTopic refinedTrendsTopic() {
        NewTopic topic = TopicBuilder.name(KafkaTopics.REFINED_SNS_YOUTUBE)
                .partitions(REFINED_TOPIC_PARTITIONS)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "compact") // 최신 트렌드만 유지
                .config("min.compaction.lag.ms", "1800000") // 30분 후 압축
                .config("compression.type", "zstd")
                .build();
        log.info("✅ Ensuring topic: name={}, partitions={}, replicas={}, policy=compact (refined)",
                KafkaTopics.REFINED_SNS_YOUTUBE, REFINED_TOPIC_PARTITIONS, defaultReplicationFactor);
        return topic;
    }

    // Error Handling Topics (짧은 보관)
    @Bean
    public NewTopic rawWikipediaDlqTopic() {
        return createDlqTopic(KafkaTopics.RAW_DOCS_WIKIPEDIA_DLQ);
    }

    @Bean
    public NewTopic rawSnsYouTubeDlqTopic() {
        return createDlqTopic(KafkaTopics.RAW_SNS_YOUTUBE_DLQ);
    }

    @Bean
    public NewTopic rawNewsYnaDlqTopic() {
        return createDlqTopic(KafkaTopics.RAW_NEWS_YNA_DLQ);
    }

    // Helper Methods
    private NewTopic createDlqTopic(String topicName) {
        NewTopic topic = TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", "604800000") // 7일 (분석용)
                .build();
        log.info("✅ Ensuring DLQ topic: name={}, partitions=1, replicas={}, retentionMs=604800000",
                topicName, defaultReplicationFactor);
        return topic;
    }

    private String hoursToMs(String hours) {
        return String.valueOf(Long.parseLong(hours) * 60 * 60 * 1000);
    }
}
