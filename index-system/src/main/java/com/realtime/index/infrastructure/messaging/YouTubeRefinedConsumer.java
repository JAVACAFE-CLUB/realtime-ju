package com.realtime.index.infrastructure.messaging;

import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.kafka.message.RefineMessage;
import com.realtime.index.application.processor.YouTubeIndexProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * YouTube refined 토픽 Consumer
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class YouTubeRefinedConsumer {

    private final YouTubeIndexProcessor processor;

    @KafkaListener(
            topics = KafkaTopics.REFINED_SNS_YOUTUBE,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "batchKafkaListenerContainerFactory",
            concurrency = "10"
    )
    public void consume(
            List<ConsumerRecord<String, RefineMessage>> records,
            Acknowledgment ack
    ) {
        if (records.isEmpty()) {
            return;
        }

        // Partition 및 offset 정보 수집
        String partitionInfo = records.stream()
                .collect(Collectors.groupingBy(ConsumerRecord::partition, Collectors.counting()))
                .entrySet().stream()
                .map(e -> "P" + e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(", "));

        long minOffset = records.stream().mapToLong(ConsumerRecord::offset).min().orElse(-1);
        long maxOffset = records.stream().mapToLong(ConsumerRecord::offset).max().orElse(-1);

        log.info("📥 Kafka Received: {} YouTube messages from partitions [{}], offset range [{}-{}]",
                records.size(), partitionInfo, minOffset, maxOffset);

        try {
            List<RefineMessage> messages = records.stream()
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());

            // RefinedId 샘플 로그 (처음 3개)
            String idSample = messages.stream()
                    .limit(3)
                    .map(RefineMessage::getRefinedId)
                    .collect(Collectors.joining(", "));
            log.info("📄 Processing refinedIds (sample): {}", idSample);

            processor.processBatch(messages);

            ack.acknowledge();
            log.info("✅ Committed {} YouTube messages successfully", messages.size());

        } catch (Exception e) {
            log.error("❌ Failed to process YouTube refined messages", e);
            throw new RuntimeException("YouTube consumer failed", e);
        }
    }
}
