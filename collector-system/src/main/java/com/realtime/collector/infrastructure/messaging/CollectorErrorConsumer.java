package com.realtime.collector.infrastructure.messaging;

import com.realtime.common.kafka.message.CollectErrorMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 수집 단계에서 발생한 DLQ(영구 실패) 이벤트를 수신하는 리스너.
 * <p> DLQ 사건을 수신하여 알람/조사 등 후속 조치에 활용합니다. </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CollectorErrorConsumer {

    @KafkaListener(
            topics = {
                    "#{T(com.realtime.common.constants.KafkaTopics).RAW_DOCS_WIKIPEDIA_DLQ}",
                    "#{T(com.realtime.common.constants.KafkaTopics).RAW_NEWS_YNA_DLQ}",
                    "#{T(com.realtime.common.constants.KafkaTopics).RAW_SNS_YOUTUBE_DLQ}"
            },
            containerFactory = "collectorContainerFactory"
    )
    public void onDlqErrorEvent(
            CollectErrorMessage event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment ack
    ) {
        try {
            log.error("[DLQ] topic={}, collectionId={}, code={}, message={}",
                    topic, event.getCollectionId(), event.getErrorCode(), event.getErrorMessage());
            handleDlqEvent(event, topic);
        } finally {
            ack.acknowledge();
        }
    }

    private void handleDlqEvent(CollectErrorMessage event, String topic) {
        // TODO: 알람/티켓 발행, 관측(Tracing) 연계, 실패 테이블 적재 등 후처리 구현
    }
}
