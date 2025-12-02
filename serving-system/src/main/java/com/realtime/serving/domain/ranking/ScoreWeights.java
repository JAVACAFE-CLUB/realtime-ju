package com.realtime.serving.domain.ranking;

import com.realtime.common.constants.ContentSource;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 키워드 스코어링 가중치 설정
 */
@Component
@ConfigurationProperties(prefix = "ranking.weights")
@Getter
@Setter
public class ScoreWeights {

    // 소스별 가중치 (YAML에서 주입됨)
    private Map<String, Double> source;

    // 시간 가중치 (하루 기준)
    private double todayWeight = 10.0;      // 당일
    private double yesterdayWeight = 5.0;   // 전일
    private double defaultWeight = 1.0;     // 그 이전

    /**
     * 소스별 가중치 반환
     */
    public double getSourceWeight(ContentSource source) {
        return this.source.getOrDefault(source.name(), 1.0);
    }

    /**
     * 시간 가중치 계산 (하루 기준)
     * - 당일: 10배
     * - 전일: 5배
     * - 그 이전: 1배
     */
    public double getTimeWeight(LocalDateTime indexedAt) {
        if (indexedAt == null) {
            return defaultWeight;
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime todayStart = now.toLocalDate().atStartOfDay();
        LocalDateTime yesterdayStart = todayStart.minusDays(1);

        if (indexedAt.isAfter(todayStart) || indexedAt.isEqual(todayStart)) {
            return todayWeight;
        } else if (indexedAt.isAfter(yesterdayStart) || indexedAt.isEqual(yesterdayStart)) {
            return yesterdayWeight;
        } else {
            return defaultWeight;
        }
    }
}
