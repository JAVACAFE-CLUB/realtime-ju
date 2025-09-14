package com.realtime.common.config;

import com.realtime.common.constants.KafkaGroups;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka 설정 (KRaft 호환)
 */
@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:realtime-ju}")
    private String defaultGroupId;

    @Value("${kafka.producer.compression-type:zstd}")
    private String compressionType;

    @Value("${kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${kafka.producer.batch-size:32768}")
    private int batchSize;

    @Value("${kafka.producer.linger-ms:10}")
    private int lingerMs;

    // ========================================
    // Enhanced Producer Configuration
    // ========================================
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // 기본 설정
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);

        // 성능 최적화
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024); // 64MB

        // 압축 설정
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        // 신뢰성 보장
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // 대용량 메시지 지원
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024); // 10MB

        // 타임아웃 설정
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());

        // 전송 완료 로깅
        template.setDefaultTopic("default-realtime-topic");

        log.info("✅ Kafka Producer initialized with compression: {}, batch-size: {}KB",
                compressionType, batchSize / 1024);

        return template;
    }

    // ========================================
    // Consumer Configuration
    // ========================================
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return createConsumerFactory(defaultGroupId);
    }

    /**
     * 그룹별 맞춤형 Consumer Factory
     */
    public ConsumerFactory<String, Object> createConsumerFactory(String groupId) {
        Map<String, Object> configProps = new HashMap<>();

        // 기본 설정
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // JSON 역직렬화 설정 (보안 강화)
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES,
                "com.realtime.common.kafka.message,com.realtime.**.dto");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true); // 타입 헤더 기반 역직렬화 활성화 (기본 타입 의존 제거)

        // 성능 최적화
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10 * 1024 * 1024);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        // 오프셋 관리
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 세션 관리 (KRaft 최적화)
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        configProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "realtime-consumer-" + groupId);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // 동시성 설정
        int concurrency = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        factory.setConcurrency(concurrency);

        // 수동 커밋 모드
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 배치 리스너 지원
        factory.setBatchListener(true);

        // 향상된 에러 핸들링
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3));
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        // 컨테이너 설정
        factory.getContainerProperties().setPollTimeout(3000);

        log.info("✅ Kafka Consumer initialized with concurrency: {}, max-poll: {}",
                concurrency, maxPollRecords);

        return factory;
    }

    /**
     * 특정 그룹용 Container Factory
     */
    @Bean("collectorContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> collectorContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(createConsumerFactory(KafkaGroups.COLLECTOR_GROUP));

        // 수집기는 낮은 동시성으로 안정성 우선
        factory.setConcurrency(2);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    @Bean("refineContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> refineContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(createConsumerFactory(KafkaGroups.REFINE_GROUP));

        // 정제 시스템은 병렬 처리량을 높임
        factory.setConcurrency(Math.max(3, Runtime.getRuntime().availableProcessors()))
        ;
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setBatchListener(true);

        return factory;
    }

    // ========================================
    // Admin Configuration
    // ========================================
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setFatalIfBrokerNotAvailable(false); // 개발환경 편의성

        return admin;
    }
}
