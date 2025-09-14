package com.realtime.collector.infrastructure.messaging;

import com.realtime.common.kafka.message.CollectErrorMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 수집 단계에서 발생한 오류 이벤트를 수신하는 리스너.
 * <p> - retry 토픽: 재시도 가능한 오류를 수신하여 모니터링/운영 트리거에 활용 </p>
 * <p> - dlq 토픽: 재시도 초과(영구 실패)를 수신하여 별도 대응(알람/조사) </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CollectorErrorEventListener {

    @KafkaListener(
            topics = "#{T(com.realtime.common.constants.KafkaTopics).RAW_SNS_YOUTUBE + '.retry'}",
            containerFactory = "collectorContainerFactory"
    )
    public void onRetryErrorEvent(CollectErrorMessage event, Acknowledgment ack) {
        try {
            log.warn("[RETRY] 수집 오류 이벤트 수신 - collectionId={}, code={}, retriable={}",
                    event.getCollectionId(), event.getErrorCode(), event.isRetriable());
            /*
             1. YT_API_ERROR
             5xx/429: 지수백오프+지터로 재시도, 최대횟수 초과 시 DLQ.
             4xx(키/권한/파라미터): 즉시 DLQ+알림.
             2. MINIO_ERROR
             네트워크/일시 오류: 업로드 재시도.
             객체 부분 성공 가능성 고려: 존재/해시 확인 후 건너뛰거나 재업로드.
             3. DB_ERROR
             Deadlock/일시 오류: 트랜잭션 재시도.
             중복키: 정책에 따라 무시 또는 upsert로 전환.
             4. KAFKA_ERROR
             프로듀서 재시도 후 실패 시 retry 토픽으로 전송, 계속 실패하면 DLQ.
             5. VALIDATION/SCHEMA/PARSE_ERROR
             데이터 자체 문제: 즉시 DLQ+알림(자동 재시도 없음).
             */
        } finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(
            topics = "#{T(com.realtime.common.constants.KafkaTopics).RAW_SNS_YOUTUBE + '.dlq'}",
            containerFactory = "collectorContainerFactory"
    )
    public void onDlqErrorEvent(CollectErrorMessage event, Acknowledgment ack) {
        try {
            log.error("[DLQ] 수집 오류 이벤트 수신 - collectionId={}, code={}, message={}",
                    event.getCollectionId(), event.getErrorCode(), event.getErrorMessage());
            // TODO: 알람/티켓 발행 또는 별도 실패 테이블 적재 등 후처리
        } finally {
            ack.acknowledge();
        }
    }
}


