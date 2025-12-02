package com.realtime.refine.application.news.yna;

import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.kafka.message.CollectMessage;
import com.realtime.refine.application.service.IdempotencyService;
import com.realtime.refine.domain.news.yna.RefinedNewsYna;
import com.realtime.refine.domain.news.yna.RefinedNewsYnaRepository;
import com.realtime.refine.infrastructure.messaging.RefineEventPublisher;
import com.realtime.refine.infrastructure.parser.jsoup.JsoupRefineService;
import com.realtime.refine.infrastructure.parser.jsoup.JsoupRefineService.RefinedText;
import com.realtime.refine.infrastructure.persistence.jpa.ContentMetadataService;
import com.realtime.refine.infrastructure.storage.minio.MinioLoader;
import com.realtime.refine.infrastructure.storage.minio.MinioUri;
import com.realtime.refine.support.HashUtils;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class YnaRefineProcessor {

    private final IdempotencyService idempotencyService;
    private final MinioLoader minioLoader;
    private final JsoupRefineService jsoupRefineService;
    private final RefinedNewsYnaRepository refinedNewsRepository;
    private final RefineEventPublisher publisher;
    private final ContentMetadataService contentMetadataService;

    public void processOne(CollectMessage msg) {
        // [1단계] 메시지에서 기본 정보 추출
        final String source = msg.getSource();
        final String contentId = msg.getCollectionId();
        final String rawDataUrl = msg.getRawDataUrl();

        log.info("🚀 정제 시작 - source={}, contentId={}", source, contentId);

        // [2단계] 중복 처리(Idempotency) 체크 - 이미 처리된 컨텐츠는 스킵
        if (idempotencyService.alreadyProcessed(source, contentId)) {
            log.info("⏭️ 이미 처리된 컨텐츠 스킵 - contentId={}", contentId);
            return;
        }

        // [3단계] MinIO URI 파싱 및 처리 시간 측정 시작
        MinioUri uri = MinioUri.parse(rawDataUrl);
        log.debug("📍 MinIO URI 파싱 완료 - bucket={}, key={}", uri.bucket(), uri.objectKey());
        long start = System.currentTimeMillis();

        try (InputStream in = minioLoader.loadStream(uri.bucket(), uri.objectKey())) {
            // [4단계] 원본 객체 크기 확인(대용량 경고)
            long size = minioLoader.sizeOf(uri.bucket(), uri.objectKey());
            log.debug("⬇️ MinIO 파일 로드 완료 - size={}bytes ({}KB)", size, size / 1024);

            if (size > 10 * 1024 * 1024) {
                log.warn("⚠️ 대용량 HTML 감지 - size={}MB, bucket={}, key={}",
                        size / (1024 * 1024), uri.bucket(), uri.objectKey());
            }

            // [5단계] HTML을 직접 읽어서 Jsoup으로 파싱 (연합뉴스는 구조화된 HTML이므로 Tika 불필요)
            long jsoupStart = System.currentTimeMillis();
            log.debug("🔄 Jsoup 파싱 시작 - contentId={}", contentId);

            // HTML을 UTF-8로 읽기
            RefinedText refined = jsoupRefineService.parseHtml(in, "UTF-8");
            long jsoupElapsed = System.currentTimeMillis() - jsoupStart;

            log.debug("✅ Jsoup 파싱 완료 - contentId={}, elapsed={}ms, title={}, contentLength={}",
                    contentId, jsoupElapsed,
                    refined.title() != null ? refined.title().substring(0, Math.min(50, refined.title().length()))
                            : "null",
                    refined.content() != null ? refined.content().length() : 0);

            // [7단계] 정제 문서 ID 및 체크섬 생성
            String refinedId = "refined_" + HashUtils.sha256Hex(source + contentId);
            String checksum = DigestUtils.sha256Hex(refined.content() != null ? refined.content() : "");
            log.debug("🔐 체크섬 생성 완료 - refinedId={}, checksum={}", refinedId, checksum.substring(0, 16) + "...");

            // [8단계] 저장할 정제 문서 도메인 객체 구성
            RefinedNewsYna doc = RefinedNewsYna.builder()
                    .id(refinedId)
                    .contentId(contentId)
                    .source(source)
                    .rawUri(rawDataUrl)
                    .title(refined.title())
                    .content(refined.content())
                    .language("ko") // 연합뉴스는 한국어
                    .charset("UTF-8")
                    .contentLength(refined.content() != null ? refined.content().length() : 0)
                    .checksum("sha256:" + checksum)
                    .refinedAt(Instant.now())
                    .processingTimeMs((int) (System.currentTimeMillis() - start))
                    .schemaVersion("1.0")
                    .processingStatus("COMPLETED")
                    .build();

            // [9단계] 기존 문서 조회 및 내용 변경 여부 판단
            log.debug("🔍 기존 문서 조회 - contentId={}", contentId);
            RefinedNewsYna existing = refinedNewsRepository.findByContentId(contentId);
            boolean contentUnchanged = false;

            if (existing != null) {
                if (existing.getChecksum() != null && existing.getChecksum().equals("sha256:" + checksum)) {
                    // [9-1단계] 내용 동일 → 업데이트 스킵, 기존 ID 유지
                    log.info("⏭️ 동일한 컨텐츠 감지 - contentId={}, 기존 문서 유지", contentId);
                    contentUnchanged = true;
                    refinedId = existing.getId();
                } else {
                    // [9-2단계] 내용 변경 → 기존 ID로 교체 저장
                    log.info("🔄 컨텐츠 변경 감지 - contentId={}, 기존 ID로 업데이트", contentId);
                    refinedId = existing.getId();
                    doc = doc.toBuilder().id(refinedId).build();
                }
            } else {
                log.debug("📝 신규 문서 - contentId={}", contentId);
            }

            // [10단계] 변경된 경우에만 저장(재시도 포함)
            boolean saved = true;
            if (!contentUnchanged) {
                log.debug("💾 MongoDB 저장 시작 - refinedId={}", refinedId);
                RefinedNewsYna finalDoc = doc;
                saved = retry(() -> refinedNewsRepository.save(finalDoc), 3, 100);
                log.debug("✅ MongoDB 저장 완료 - refinedId={}", refinedId);
            }

            // [11단계] 저장 성공 시 중복 처리 마커 등록(Idempotency) 및 MySQL 메타데이터 업데이트
            if (saved) {
                idempotencyService.markProcessed(source, contentId, refinedId);
                log.debug("✅ Idempotency 마커 등록 완료 - contentId={}", contentId);

                // MySQL 메타데이터 업데이트 (트랜잭션 처리됨)
                contentMetadataService.updateRefinedId(refinedId, source, contentId);
            }

            // [12단계] 정제 완료 이벤트 발행 → 인덱싱 등 다음 단계 트리거
            log.debug("📤 Kafka 이벤트 발행 - topic={}, refinedId={}", KafkaTopics.REFINED_NEWS_YNA, refinedId);
            publisher.publishRefined(
                    KafkaTopics.REFINED_NEWS_YNA,
                    refinedId,
                    refinedId,
                    contentId,
                    source
            );

            long totalElapsed = System.currentTimeMillis() - start;
            log.info(
                    "✅ 정제 완료 - source={}, contentId={}, refinedId={}, totalTime={}ms, jsoupTime={}ms, contentLength={}",
                    source, contentId, refinedId, totalElapsed, jsoupElapsed,
                    refined.content() != null ? refined.content().length() : 0);

        } catch (Exception e) {
            // [에러 처리] 예외 로깅 및 DLQ로 에러 이벤트 발행(배치 지속)
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ 정제 실패 - source={}, contentId={}, elapsed={}ms, errorType={}, error={}",
                    source, contentId, elapsed, classifyError(e), e.getMessage(), e);

            publisher.publishRefineError(
                    KafkaTopics.REFINED_NEWS_YNA_DLQ,
                    contentId,
                    contentId,
                    source,
                    rawDataUrl,
                    classifyError(e),
                    e.getMessage(),
                    false
            );
        }
    }

    private String classifyError(Exception e) {
        String n = e.getClass().getSimpleName();
        if (n.contains("Timeout") || n.contains("SocketTimeout")) {
            return "TIKA_TIMEOUT";
        }
        if (n.contains("Minio") || n.contains("S3")) {
            return "MINIO_ERROR";
        }
        if (n.contains("Mongo") || n.contains("Bson")) {
            return "MONGO_ERROR";
        }
        return "REFINE_ERROR";
    }

    private static <T> T retry(Callable<T> callable, int maxAttempts, long backoffMs)
            throws Exception {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (++attempt >= maxAttempts) {
                    throw e;
                }
                try {
                    Thread.sleep(backoffMs * attempt);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
