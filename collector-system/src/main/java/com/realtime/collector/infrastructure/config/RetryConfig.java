package com.realtime.collector.infrastructure.config;

import com.realtime.collector.exception.RetriableException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Configuration
@EnableRetry
public class RetryConfig {

    /**
     * 기사 크롤링용 재시도 정책
     */
    @Bean
    public RetryPolicy articleCrawlRetryPolicy() {
        // 예외별 재시도 정책
        Map<Class<? extends Throwable>, RetryPolicy> policyMap = new HashMap<>();

        // 재시도 가능한 예외
        policyMap.put(WebClientRequestException.class, maxAttemptsPolicy(3));
        policyMap.put(SocketTimeoutException.class, maxAttemptsPolicy(3));
        policyMap.put(WebClientResponseException.TooManyRequests.class, maxAttemptsPolicy(5));
        policyMap.put(WebClientResponseException.ServiceUnavailable.class, maxAttemptsPolicy(3));
        policyMap.put(WebClientResponseException.GatewayTimeout.class, maxAttemptsPolicy(3));
        policyMap.put(RetriableException.class, maxAttemptsPolicy(3));

        // 재시도 불가능한 예외
        policyMap.put(WebClientResponseException.NotFound.class, new NeverRetryPolicy());
        policyMap.put(WebClientResponseException.Forbidden.class, new NeverRetryPolicy());
        policyMap.put(IllegalArgumentException.class, new NeverRetryPolicy());

        ExceptionClassifierRetryPolicy policy = new ExceptionClassifierRetryPolicy();
        policy.setExceptionClassifier(new Classifier<Throwable, RetryPolicy>() {
            @Override
            public RetryPolicy classify(Throwable throwable) {
                return policyMap.getOrDefault(
                        throwable.getClass(),
                        new NeverRetryPolicy() // 기본: 재시도 안함
                );
            }
        });

        return policy;
    }

    /**
     * 백오프 정책: 지수 증가
     */
    @Bean
    public ExponentialBackOffPolicy articleCrawlBackOffPolicy() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);      // 1초
        backOffPolicy.setMultiplier(2.0);            // 2배씩 증가
        backOffPolicy.setMaxInterval(10000);         // 최대 10초
        return backOffPolicy;
    }

    private MaxAttemptsRetryPolicy maxAttemptsPolicy(int maxAttempts) {
        MaxAttemptsRetryPolicy policy = new MaxAttemptsRetryPolicy();
        policy.setMaxAttempts(maxAttempts);
        return policy;
    }
}