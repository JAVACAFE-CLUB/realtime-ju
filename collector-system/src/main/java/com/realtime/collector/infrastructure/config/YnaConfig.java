package com.realtime.collector.infrastructure.config;

import java.util.List;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class YnaConfig {

    @Value("${yna.feeds:https://www.yna.co.kr/rss/news.xml}")
    private List<String> feeds;

    @Value("${yna.concurrency:4}")
    private int concurrency;

    @Value("${yna.inter-request-delay-ms:250}")
    private int interRequestDelayMs;

    @Value("${yna.inter-request-jitter-ms:150}")
    private int interRequestJitterMs;

    @Value("${yna.retry.max-attempts:2}")
    private int maxRetries;

    @Value("${yna.retry.base-backoff-ms:200}")
    private int baseBackoffMs;

    @Value("${yna.http.connect-timeout-ms:2000}")
    private int connectTimeoutMs;

    @Value("${yna.http.response-timeout-ms:5000}")
    private int responseTimeoutMs;

    @Value("${yna.http.user-agent:Mozilla/5.0 (compatible; YnaCollector/1.0; +https://example.com/bot)}")
    private String userAgent;

    @Value("${yna.http.accept-language:ko-KR,ko;q=0.9}")
    private String acceptLanguage;
}


