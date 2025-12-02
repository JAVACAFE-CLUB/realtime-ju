package com.realtime.refine.application.sns.youtube;

import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.kafka.message.CollectMessage;
import com.realtime.refine.application.service.IdempotencyService;
import com.realtime.refine.domain.sns.youtube.RefinedSnsYouTube;
import com.realtime.refine.domain.sns.youtube.RefinedSnsYouTubeRepository;
import com.realtime.refine.infrastructure.messaging.RefineEventPublisher;
import com.realtime.refine.infrastructure.parser.json.JsonRefineService;
import com.realtime.refine.infrastructure.persistence.jpa.ContentMetadataService;
import com.realtime.refine.infrastructure.storage.minio.MinioLoader;
import com.realtime.refine.infrastructure.storage.minio.MinioUri;
import com.realtime.refine.support.HashUtils;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class YouTubeRefineProcessor {

    private final IdempotencyService idempotencyService;
    private final MinioLoader minioLoader;
    private final JsonRefineService jsonRefineService;
    private final RefinedSnsYouTubeRepository refinedYouTubeRepository;
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
            // [4단계] 원본 객체 크기 확인
            long size = minioLoader.sizeOf(uri.bucket(), uri.objectKey());
            log.debug("⬇️ MinIO 파일 로드 완료 - size={}bytes ({}KB)", size, size / 1024);

            // [5단계] JSON 파싱하여 YouTubeVideo 객체 생성
            long jsonStart = System.currentTimeMillis();
            log.debug("🔄 JSON 파싱 시작 - contentId={}", contentId);

            YouTubeVideo video = jsonRefineService.parseJson(in, YouTubeVideo.class);
            long jsonElapsed = System.currentTimeMillis() - jsonStart;

            log.debug("✅ JSON 파싱 완료 - contentId={}, elapsed={}ms, videoId={}",
                    contentId, jsonElapsed, video.id());

            // [6단계] 필드 추출 및 정제
            String title = extractTitle(video);
            String description = extractDescription(video);
            String tags = extractTags(video);
            String channelId = video.snippet() != null ? video.snippet().channelId() : "";
            String channelTitle = video.snippet() != null ? video.snippet().channelTitle() : "";
            String categoryId = video.snippet() != null ? video.snippet().categoryId() : "";
            String publishedAt = video.snippet() != null ? video.snippet().publishedAt() : "";
            String thumbnailUrl = extractThumbnailUrl(video);
            String language = extractLanguage(video);

            log.debug("📝 필드 추출 완료 - title={}, descriptionLength={}, tags={}",
                    title != null ? title.substring(0, Math.min(50, title.length())) : "null",
                    description != null ? description.length() : 0,
                    tags);

            // [7단계] 정제 문서 ID 및 체크섬 생성
            String refinedId = "refined_" + HashUtils.sha256Hex(source + contentId);
            String checksumInput = (title != null ? title : "") +
                    (description != null ? description : "") +
                    (tags != null ? tags : "");
            String checksum = DigestUtils.sha256Hex(checksumInput);
            log.debug("🔐 체크섬 생성 완료 - refinedId={}, checksum={}", refinedId, checksum.substring(0, 16) + "...");

            // [8단계] 저장할 정제 문서 도메인 객체 구성
            int contentLength = (title != null ? title.length() : 0) +
                    (description != null ? description.length() : 0);

            RefinedSnsYouTube doc = RefinedSnsYouTube.builder()
                    .id(refinedId)
                    .contentId(contentId)
                    .source(source)
                    .rawUri(rawDataUrl)
                    .title(title)
                    .description(description)
                    .tags(tags)
                    .channelId(channelId)
                    .channelTitle(channelTitle)
                    .categoryId(categoryId)
                    .publishedAt(publishedAt)
                    .thumbnailUrl(thumbnailUrl)
                    .language(language)
                    .charset("UTF-8")
                    .contentLength(contentLength)
                    .checksum("sha256:" + checksum)
                    .refinedAt(Instant.now())
                    .processingTimeMs((int) (System.currentTimeMillis() - start))
                    .schemaVersion("1.0")
                    .processingStatus("COMPLETED")
                    .build();

            // [9단계] 기존 문서 조회 및 내용 변경 여부 판단
            log.debug("🔍 기존 문서 조회 - contentId={}", contentId);
            RefinedSnsYouTube existing = refinedYouTubeRepository.findByContentId(contentId);
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
                RefinedSnsYouTube finalDoc = doc;
                saved = retry(() -> refinedYouTubeRepository.save(finalDoc), 3, 100);
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
            log.debug("📤 Kafka 이벤트 발행 - topic={}, refinedId={}", KafkaTopics.REFINED_SNS_YOUTUBE, refinedId);
            publisher.publishRefined(
                    KafkaTopics.REFINED_SNS_YOUTUBE,
                    refinedId,
                    refinedId,
                    contentId,
                    source
            );

            long totalElapsed = System.currentTimeMillis() - start;
            log.info(
                    "✅ 정제 완료 - source={}, contentId={}, refinedId={}, totalTime={}ms, jsonTime={}ms, contentLength={}",
                    source, contentId, refinedId, totalElapsed, jsonElapsed, contentLength);

        } catch (Exception e) {
            // [에러 처리] 예외 로깅 및 DLQ로 에러 이벤트 발행(배치 지속)
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ 정제 실패 - source={}, contentId={}, elapsed={}ms, errorType={}, error={}",
                    source, contentId, elapsed, classifyError(e), e.getMessage(), e);

            publisher.publishRefineError(
                    KafkaTopics.REFINED_SNS_YOUTUBE_DLQ,
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

    private String extractTitle(YouTubeVideo video) {
        if (video.snippet() == null) {
            return "";
        }

        // localized 제목이 있으면 우선 사용
        if (video.snippet().localized() != null && video.snippet().localized().title() != null) {
            return jsonRefineService.normalizeText(video.snippet().localized().title());
        }

        // 기본 제목 사용
        if (video.snippet().title() != null) {
            return jsonRefineService.normalizeText(video.snippet().title());
        }

        return "";
    }

    private String extractDescription(YouTubeVideo video) {
        if (video.snippet() == null) {
            return "";
        }

        // localized 설명이 있으면 우선 사용
        if (video.snippet().localized() != null && video.snippet().localized().description() != null) {
            return jsonRefineService.normalizeText(video.snippet().localized().description());
        }

        // 기본 설명 사용
        if (video.snippet().description() != null) {
            return jsonRefineService.normalizeText(video.snippet().description());
        }

        return "";
    }

    private String extractTags(YouTubeVideo video) {
        if (video.snippet() == null || video.snippet().tags() == null || video.snippet().tags().isEmpty()) {
            return "";
        }

        // 태그 배열을 쉼표로 구분된 문자열로 변환
        return video.snippet().tags().stream()
                .filter(tag -> tag != null && !tag.trim().isEmpty())
                .collect(Collectors.joining(", "));
    }

    private String extractThumbnailUrl(YouTubeVideo video) {
        if (video.snippet() == null || video.snippet().thumbnails() == null) {
            return "";
        }

        var thumbnails = video.snippet().thumbnails();

        // 우선순위: high > medium > default
        if (thumbnails.high() != null && thumbnails.high().url() != null) {
            return thumbnails.high().url();
        }
        if (thumbnails.medium() != null && thumbnails.medium().url() != null) {
            return thumbnails.medium().url();
        }
        if (thumbnails.defaultThumbnail() != null && thumbnails.defaultThumbnail().url() != null) {
            return thumbnails.defaultThumbnail().url();
        }

        return "";
    }

    private String extractLanguage(YouTubeVideo video) {
        if (video.snippet() == null) {
            return "unknown";
        }

        // defaultAudioLanguage 또는 defaultLanguage 사용
        if (video.snippet().defaultAudioLanguage() != null) {
            return video.snippet().defaultAudioLanguage();
        }
        if (video.snippet().defaultLanguage() != null) {
            return video.snippet().defaultLanguage();
        }

        return "unknown";
    }

    private String classifyError(Exception e) {
        String n = e.getClass().getSimpleName();
        if (n.contains("Minio") || n.contains("S3")) {
            return "MINIO_ERROR";
        }
        if (n.contains("Mongo") || n.contains("Bson")) {
            return "MONGO_ERROR";
        }
        if (n.contains("Json") || n.contains("Jackson")) {
            return "JSON_PARSE_ERROR";
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

    // ============================================================
    // YouTubeVideo DTO (collector-system의 DTO와 동일한 구조)
    // ============================================================

    public record YouTubeVideo(
            String kind,
            String etag,
            String id,
            Snippet snippet
    ) {}

    public record Snippet(
            String publishedAt,
            String channelId,
            String title,
            String description,
            Thumbnails thumbnails,
            String channelTitle,
            List<String> tags,
            String categoryId,
            String liveBroadcastContent,
            String defaultLanguage,
            Localized localized,
            String defaultAudioLanguage
    ) {}

    public record Thumbnails(
            Thumbnail defaultThumbnail,
            Thumbnail medium,
            Thumbnail high,
            Thumbnail standard,
            Thumbnail maxres
    ) {}

    public record Thumbnail(
            String url,
            Integer width,
            Integer height
    ) {}

    public record Localized(
            String title,
            String description
    ) {}
}
