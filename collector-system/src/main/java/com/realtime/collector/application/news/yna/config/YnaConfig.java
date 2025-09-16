package com.realtime.collector.application.news.yna.config;

import java.util.List;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class YnaConfig {

    @Value("${collector.yna.feeds:https://www.yna.co.kr/rss/news.xml}")
    private List<String> feeds;

    @Value("${collector.yna.concurrency:4}")
    private int concurrency;

    @Value("${collector.yna.inter-request-delay-ms:250}")
    private int interRequestDelayMs;

    @Value("${collector.yna.inter-request-jitter-ms:150}")
    private int interRequestJitterMs;

    @Value("${collector.yna.retry.max-attempts:2}")
    private int maxRetries;

    @Value("${collector.yna.retry.base-backoff-ms:200}")
    private int baseBackoffMs;

    @Value("${collector.yna.http.connect-timeout-ms:2000}")
    private int connectTimeoutMs;

    @Value("${collector.yna.http.response-timeout-ms:5000}")
    private int responseTimeoutMs;

    @Value("${collector.yna.http.user-agent:Mozilla/5.0 (compatible; YnaCollector/1.0; +https://example.com/bot)}")
    private String userAgent;

    @Value("${collector.yna.http.accept-language:ko-KR,ko;q=0.9}")
    private String acceptLanguage;
}
