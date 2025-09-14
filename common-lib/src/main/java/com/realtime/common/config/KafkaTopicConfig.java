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
 */
@Configuration
@Slf4j
public class KafkaTopicConfig {

    @Value("${kafka.topic.partitions:3}")
    private int defaultPartitions;

    @Value("${kafka.topic.replication-factor:1}")
    private short defaultReplicationFactor;

    @Value("${kafka.topic.retention.raw-data-hours:168}") // 7일
    private String rawDataRetentionHours;

    @Value("${kafka.topic.retention.refined-data-hours:336}") // 14일
    private String refinedDataRetentionHours;

    // ========================================
    // 🔥 Raw Data Topics (원본 데이터)
    // ========================================
    @Bean
    public NewTopic rawWikipediaTopic() {
        return TopicBuilder.name(KafkaTopics.RAW_DOCS_WIKIPEDIA)
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", hoursToMs(rawDataRetentionHours))
                .config("segment.ms", "86400000") // 1일
                .config("compression.type", "zstd")
                .config("max.message.bytes", "10485760") // 10MB
                .config("min.insync.replicas", "1")
                .build();
    }

    @Bean
    public NewTopic rawNewsNaverTopic() {
        return TopicBuilder.name(KafkaTopics.RAW_NEWS_NAVER)
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", hoursToMs(rawDataRetentionHours))
                .config("segment.ms", "3600000") // 1시간
                .config("compression.type", "zstd")
                .config("max.message.bytes", "5242880") // 5MB
                .build();
    }

    @Bean
    public NewTopic rawSnsYouTubeTopic() {
        return TopicBuilder.name(KafkaTopics.RAW_SNS_YOUTUBE)
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", hoursToMs(rawDataRetentionHours))
                .config("segment.ms", "3600000") // 1시간
                .config("compression.type", "zstd")
                .config("max.message.bytes", "10485760") // 10MB (동영상 메타데이터 대비)
                .build();
    }

    // ========================================
    // 🔥 Refined Data Topics (정제된 데이터)
    // ========================================
    @Bean
    public NewTopic refinedArticlesTopic() {
        return TopicBuilder.name(KafkaTopics.REFINED_DOCS_WIKIPEDIA)
                .partitions(5) // 더 많은 파티션으로 처리량 향상
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", hoursToMs(refinedDataRetentionHours))
                .config("compression.type", "zstd")
                .build();
    }

    @Bean
    public NewTopic refinedKeywordsTopic() {
        return TopicBuilder.name(KafkaTopics.REFINED_NEWS_NAVER)
                .partitions(5)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "compact") // 키워드는 압축 정책
                .config("min.compaction.lag.ms", "3600000") // 1시간 후 압축
                .config("compression.type", "zstd")
                .build();
    }

    @Bean
    public NewTopic refinedTrendsTopic() {
        return TopicBuilder.name(KafkaTopics.REFINED_SNS_YOUTUBE)
                .partitions(3)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "compact") // 최신 트렌드만 유지
                .config("min.compaction.lag.ms", "1800000") // 30분 후 압축
                .config("compression.type", "zstd")
                .build();
    }

    // ========================================
    // 🔥 Error Handling Topics (짧은 보관)
    // ========================================
    @Bean
    public NewTopic rawWikipediaRetryTopic() {
        return createRetryTopic(KafkaTopics.RAW_DOCS_WIKIPEDIA_RETRY);
    }

    @Bean
    public NewTopic rawWikipediaDlqTopic() {
        return createDlqTopic(KafkaTopics.RAW_DOCS_WIKIPEDIA_DLQ);
    }

    @Bean
    public NewTopic rawNewsNaverRetryTopic() {
        return createRetryTopic(KafkaTopics.RAW_NEWS_NAVER_RETRY);
    }

    @Bean
    public NewTopic rawNewsNaverDlqTopic() {
        return createDlqTopic(KafkaTopics.RAW_NEWS_NAVER_DLQ);
    }


    @Bean
    public NewTopic rawSnsYouTubeRetryTopic() {
        return createRetryTopic(KafkaTopics.RAW_SNS_YOUTUBE_RETRY);
    }

    @Bean
    public NewTopic rawSnsYouTubeDlqTopic() {
        return createDlqTopic(KafkaTopics.RAW_SNS_YOUTUBE_DLQ);
    }

    // ========================================
    // 🔥 Helper Methods
    // ========================================
    private NewTopic createRetryTopic(String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(1) // 재시도는 순서 보장
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", "259200000") // 3일
                .build();
    }

    private NewTopic createDlqTopic(String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(defaultReplicationFactor)
                .config("cleanup.policy", "delete")
                .config("retention.ms", "604800000") // 7일 (분석용)
                .build();
    }

    private String hoursToMs(String hours) {
        return String.valueOf(Long.parseLong(hours) * 60 * 60 * 1000);
    }
}
