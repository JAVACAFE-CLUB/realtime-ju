package com.realtime.serving.presentation.rest.ranking;

import com.realtime.serving.application.dto.KeywordRankingResponse;
import com.realtime.serving.application.service.KeywordRankingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 키워드 랭킹 조회 API
 */
@RestController
@RequestMapping("/api/v1/keywords")
@RequiredArgsConstructor
@Slf4j
public class KeywordRankingController {

    private final KeywordRankingService keywordRankingService;

    /**
     * 실시간 트렌딩 키워드 랭킹 조회
     *
     * @param limit 조회할 키워드 개수 (기본: 10)
     * @return 랭킹 목록
     */
    @GetMapping("/trending")
    public ResponseEntity<KeywordRankingResponse> getTrendingKeywords(
            @RequestParam(defaultValue = "10") int limit
    ) {
        log.info("GET /api/v1/keywords/trending?limit={}", limit);

        if (limit < 1 || limit > 100) {
            return ResponseEntity.badRequest().build();
        }

        KeywordRankingResponse response = keywordRankingService.getTopKeywords(limit);
        return ResponseEntity.ok(response);
    }

    /**
     * 상위 10개 트렌딩 키워드 조회 (기본)
     */
    @GetMapping("/trending/top10")
    public ResponseEntity<KeywordRankingResponse> getTop10Keywords() {
        log.info("GET /api/v1/keywords/trending/top10");
        KeywordRankingResponse response = keywordRankingService.getTopKeywords();
        return ResponseEntity.ok(response);
    }
}
