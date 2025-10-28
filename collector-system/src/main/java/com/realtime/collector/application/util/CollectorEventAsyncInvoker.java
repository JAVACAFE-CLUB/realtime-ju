package com.realtime.collector.application.util;

import com.realtime.collector.infrastructure.messaging.CollectorEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class CollectorEventAsyncInvoker {

    private final CollectorEventPublisher collectorEventPublisher;

    @Async("kafkaTaskExecutor")
    public void publishSuccess(String source, String topic, String collectionId, String minioUrl, int videoCount) {
        collectorEventPublisher.publishCollected(source, topic, collectionId, minioUrl, videoCount)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("📣 성공 이벤트 발행 완료 - CollectionId: {}", collectionId);
                    } else {
                        log.error("📣 성공 이벤트 발행 실패 - CollectionId: {}", collectionId, ex);
                    }
                });
    }

    @Async("kafkaTaskExecutor")
    public void publishError(String source, String topic, String collectionId, String rawDataUrl,
                             String errorCode, String message, boolean retriable) {
        collectorEventPublisher.publishCollectError(source, topic, collectionId, rawDataUrl, errorCode, message,
                        retriable)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.warn("🚨 오류 이벤트 발행 완료 - CollectionId: {}", collectionId);
                    } else {
                        log.error("🚨 오류 이벤트 발행 실패 - CollectionId: {}", collectionId, ex);
                    }
                });
    }
}
