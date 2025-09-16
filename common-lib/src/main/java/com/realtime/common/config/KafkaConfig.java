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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
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

        log.info("✅ Kafka Producer initialized with compression: {}, batch-size: {}KB",
                compressionType, batchSize / 1024);

        return template;
    }

    /**
     * 그룹별 공통 ConsumerFactory 생성
     * - 수동 커밋, 타입 헤더 기반 JSON 역직렬화, KRaft 타임아웃 최적화 적용
     */
    public ConsumerFactory<String, Object> createConsumerFactory(String groupId) {
        Map<String, Object> configProps = new HashMap<>();

        // 기본 설정
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // ErrorHandlingDeserializer로 감싸 역직렬화 오류를 컨테이너 에러 핸들러로 위임
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // JSON 역직렬화 설정 (보안 강화)
        // 패키지 패턴을 보다 명시적으로 확장하여 하위 패키지 신뢰
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.realtime.*");
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

    /**
     * 특정 그룹용 Container Factory
     */
    @Bean("collectorContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> collectorContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(createConsumerFactory(KafkaGroups.COLLECTOR_GROUP));

        // 수집기는 낮은 동시성으로 안정성 우선
        int concurrency = 2;
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // 공통 에러 핸들러 적용
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3));
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        log.info("✅ Kafka collector container initialized: concurrency={}, ack={}, batch={}",
                concurrency,
                factory.getContainerProperties().getAckMode(),
                factory.isBatchListener());

        return factory;
    }

    @Bean("refineContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> refineContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(createConsumerFactory(KafkaGroups.REFINE_GROUP));

        // 정제 시스템은 병렬 처리량을 높임
        int concurrency = Math.max(3, Runtime.getRuntime().availableProcessors());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setBatchListener(true);

        // 공통 에러 핸들러 적용
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3));
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        log.info("✅ Kafka refine container initialized: concurrency={}, ack={}, batch={}",
                concurrency,
                factory.getContainerProperties().getAckMode(),
                factory.isBatchListener());

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

        log.info("✅ KafkaAdmin initialized (bootstrapServers={})", bootstrapServers);

        return admin;
    }
}
