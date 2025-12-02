package com.realtime.refine.infrastructure.messaging;

import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.kafka.message.CollectMessage;
import com.realtime.refine.application.sns.youtube.YouTubeRefineProcessor;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class YouTubeRawSnsBatchConsumer {

    private final YouTubeRefineProcessor processor;

    // [수신 설정] RAW_SNS_YOUTUBE 토픽에서 배치 리스닝
    @KafkaListener(
            topics = KafkaTopics.RAW_SNS_YOUTUBE,
            containerFactory = "refineContainerFactory"
    )
    public void onBatch(List<ConsumerRecord<String, CollectMessage>> records, Acknowledgment ack) {

        // [1단계] 빈 배치 방어코드
        if (records == null || records.isEmpty()) {
            log.debug("⏭️ 빈 배치 수신, 처리 스킵");
            return;
        }

        // [2단계] 배치 정보 로깅
        log.info("📥 YouTube 배치 수신 - size={}, partitions={}",
                records.size(),
                records.stream().map(ConsumerRecord::partition).distinct().count());

        // [3단계] 처리 시간 측정 시작
        long start = System.currentTimeMillis();
        int successCount = 0;
        int failCount = 0;

        try {
            // [4단계] 배치 처리 (성공 시에만 오프셋 커밋)
            log.debug("🔄 배치 처리 시작 - size={}", records.size());

            // [4-1단계] 각 메시지 순차 처리 → 정제 유스케이스 호출
            for (var r : records) {
                try {
                    processor.processOne(r.value());
                    successCount++;
                } catch (Exception e) {
                    failCount++;
                    log.warn("⚠️ 메시지 처리 실패 - partition={}, offset={}, key={}, error={}",
                            r.partition(), r.offset(), r.key(), e.getMessage());
                }
            }

            // [4-2단계] 수동 오프셋 커밋
            log.debug("💾 오프셋 커밋 - processedCount={}", successCount);
            ack.acknowledge();
            log.info("✅ 배치 처리 및 커밋 완료 - size={}, success={}, fail={}",
                    records.size(), successCount, failCount);

        } catch (Exception e) {
            // [에러 처리] 배치 처리 실패 로그 (DLQ 처리는 유스케이스에서 수행)
            log.error("❌ YouTube 배치 처리 실패 - size={}, error={}", records.size(), e.getMessage(), e);

        } finally {
            // [마감] 처리 소요시간 및 배치 크기 기록
            long took = System.currentTimeMillis() - start;
            double avgMs = records.isEmpty() ? 0 : (double) took / records.size();
            log.info("⏱️ YouTube 배치 처리 완료 - size={}, success={}, fail={}, totalTime={}ms, avgTime={:.2f}ms/msg",
                    records.size(), successCount, failCount, took, avgMs);
        }
    }
}
