package com.realtime.common.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

@Configuration
@ConditionalOnProperty(prefix = "webclient", name = "enabled", havingValue = "true", matchIfMissing = true)
public class WebClientConfig {

    @Bean
    @ConditionalOnMissingBean
    public WebClient webClient() {
        ConnectionProvider provider = ConnectionProvider.builder("wc-pool")
            .maxConnections(200)
            .pendingAcquireMaxCount(1000)
            .build();

        HttpClient httpClient = HttpClient.create(provider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 8000)
            .responseTimeout(Duration.ofSeconds(15))
            .doOnConnected(conn -> conn
                .addHandlerLast(new ReadTimeoutHandler(15))
                .addHandlerLast(new WriteTimeoutHandler(10))
            );

        ExchangeStrategies exchangeStrategies = ExchangeStrategies.builder()
            .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
            .build();

        return WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .exchangeStrategies(exchangeStrategies)
            .build();
    }
}


