package com.realtime.serving.domain.ranking;

import com.realtime.common.constants.ContentSource;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
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

    // 시간 가중치
    private double recentHourWeight = 10.0;
    private double recentDayWeight = 5.0;
    private double defaultWeight = 1.0;

    /**
     * 소스별 가중치 반환
     */
    public double getSourceWeight(ContentSource source) {
        return this.source.getOrDefault(source.name(), 1.0);
    }

    /**
     * 시간 가중치 계산
     * - 최근 1시간: 10배
     * - 최근 24시간: 5배
     * - 그 이상: 1배
     */
    public double getTimeWeight(LocalDateTime indexedAt) {
        if (indexedAt == null) {
            return defaultWeight;
        }

        Duration duration = Duration.between(indexedAt, LocalDateTime.now());
        long hours = duration.toHours();

        if (hours < 1) {
            return recentHourWeight;
        } else if (hours < 24) {
            return recentDayWeight;
        } else {
            return defaultWeight;
        }
    }
}
