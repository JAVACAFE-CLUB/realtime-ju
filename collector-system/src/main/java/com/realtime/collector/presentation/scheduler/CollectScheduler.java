package com.realtime.collector.presentation.scheduler;

import com.realtime.collector.application.news.yna.YnaCollector;
import com.realtime.collector.application.sns.youtube.YouTubeCollector;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class CollectScheduler {

    private final YouTubeCollector youTubeCollector;
    private final YnaCollector ynaCollector;

    @Scheduled(cron = "0 30 * * * *")
    public void scheduleYouTubeCollect() {
        CompletableFuture<Void> future = youTubeCollector.collectAndProcessYouTubeData();
        future.exceptionally(ex -> {
            log.error("스케줄 실행 중 예외", ex);
            return null;
        });
    }

    @Scheduled(cron = "0 40 * * * *")
    public void scheduleYnaCollect() {
        CompletableFuture<Void> future = ynaCollector.collectAndProcessYnaData();
        future.exceptionally(ex -> {
            log.error("YNA 스케줄 실행 중 예외", ex);
            return null;
        });
    }
}
