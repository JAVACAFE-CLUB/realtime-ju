package com.realtime.collector.presentation.rest.collect;

import com.realtime.collector.application.sns.youtube.YouTubeCollector;
import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 수동 트리거용 수집 컨트롤러.
 * <p>
 * 목적: - 스케줄 대기 없이 즉시 수집 작업을 시작하기 위한 엔드포인트를 제공합니다.
 * <p>
 * 동작: - 요청을 수신하면 비동기로 수집을 시작하고, 즉시 202 Accepted 응답을 반환합니다.
 * <p>
 * 보안/운영 주의: - 운영 환경에서는 인증/인가(예: API 토큰) 적용 또는 프로필 기반 비활성화를 권장합니다. - 스케줄러와의 중복 실행을 피하기 위해 호출 빈도를 제어하세요.
 */
@RestController
@RequestMapping("/collect")
@RequiredArgsConstructor
@Slf4j
public class ManualCollectController {

    private final YouTubeCollector youTubeCollector;

    @PostMapping("/youtube")
    public Mono<ResponseEntity<Map<String, Object>>> triggerYouTubeCollectPost() {
        youTubeCollector.collectAndProcessYouTubeData();
        return Mono.just(ResponseEntity.accepted().body(Map.of(
                "status", "accepted",
                "message", "YouTube collection started",
                "acceptedAt", Instant.now().toString()
        )));
    }

}


