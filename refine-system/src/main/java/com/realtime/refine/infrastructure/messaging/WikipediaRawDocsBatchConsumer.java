package com.realtime.refine.infrastructure.messaging;

import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.kafka.message.CollectMessage;
import com.realtime.refine.application.docs.wikipedia.WikipediaRefineProcessor;
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
public class WikipediaRawDocsBatchConsumer {

    private final WikipediaRefineProcessor processor;

    @org.springframework.beans.factory.annotation.Value("${refine.consumer.delay-ms:0}")
    private long delayMs;

    // [수신 설정] RAW_DOCS_WIKIPEDIA 토픽에서 배치 리스닝
    @KafkaListener(topics = KafkaTopics.RAW_DOCS_WIKIPEDIA, containerFactory = "refineContainerFactory")
    public void onBatch(List<ConsumerRecord<String, CollectMessage>> records, Acknowledgment ack) {
        // Consumer 메서드 호출 확인용 로그
        log.info("🔔 WikipediaRawDocsBatchConsumer.onBatch() 호출됨 - records.size()={}",
                records != null ? records.size() : 0);

        // [1단계] 빈 배치 방어코드
        if (records == null || records.isEmpty()) {
            log.info("⏭️ 빈 배치 수신, 처리 스킵");
            // 빈 배치는 Spring Kafka가 자동으로 처리하므로 명시적 commit 불필요
            return;
        }

        // [2단계] 배치 정보 로깅
        log.info("📥 위키피디아 샤드 배치 수신 - size={}, partitions={}, firstOffset={}, lastOffset={}",
                records.size(),
                records.stream().map(ConsumerRecord::partition).distinct().count(),
                records.get(0).offset(),
                records.get(records.size() - 1).offset());

        // [3단계] 처리 시간 측정 시작
        long start = System.currentTimeMillis();
        int successCount = 0;
        int failCount = 0;
        boolean committed = false;

        try {
            // [4단계] 배치 처리
            log.debug("🔄 배치 처리 시작 - size={}", records.size());

            if (delayMs > 0) {
                try {
                    log.info("⏳ 테스트용 지연 적용: {}ms", delayMs);
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("⚠️ 지연 중 인터럽트 발생");
                }
            }

            // [4-1단계] 각 메시지 순차 처리 → 정제 유스케이스 호출
            for (var r : records) {
                try {
                    processor.processShard(r.value());
                    successCount++;
                } catch (Exception e) {
                    failCount++;
                    log.warn("⚠️ 샤드 처리 실패 - partition={}, offset={}, key={}, error={}",
                            r.partition(), r.offset(), r.key(), e.getMessage());
                    // 개별 메시지 실패는 계속 진행 (배치 전체 실패 아님)
                }
            }

            // [4-2단계] 수동 오프셋 커밋
            // 개별 메시지 실패와 관계없이 배치 전체를 commit하여 재처리 방지
            // (실패한 메시지는 로그로 기록되었으므로 별도 처리 가능)
            log.debug("💾 오프셋 커밋 - processedCount={}, success={}, fail={}",
                    records.size(), successCount, failCount);
            ack.acknowledge();
            committed = true;
            log.info("✅ 배치 처리 및 커밋 완료 - size={}, success={}, fail={}",
                    records.size(), successCount, failCount);

        } catch (Exception e) {
            // [에러 처리] 배치 처리 실패 로그
            // 예외 발생 시 offset을 commit하지 않아 재처리 가능하도록 함
            // (무한 재처리 방지를 위해 ErrorHandler에서 처리하거나 DLQ로 전송)
            log.error("❌ 위키피디아 샤드 배치 처리 실패 - size={}, error={}",
                    records.size(), e.getMessage(), e);
            // 예외를 다시 throw하여 Spring Kafka ErrorHandler가 처리하도록 함
            throw e;

        } finally {
            // [마감] 처리 소요시간 및 배치 크기 기록
            long took = System.currentTimeMillis() - start;
            double avgMs = records.isEmpty() ? 0 : (double) took / records.size();
            log.info(
                    "⏱️ 위키피디아 샤드 배치 처리 완료 - size={}, success={}, fail={}, totalTime={}ms, avgTime={:.2f}ms/shard, committed={}",
                    records.size(), successCount, failCount, took, avgMs, committed);
        }
    }
}
