package com.realtime.collector.application.news.yna.dto;

import com.realtime.common.constants.ContentSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Manifest {

    @Builder.Default
    private int manifestVersion = 1;
    @Builder.Default
    private String source = ContentSource.NEWS_YNA.name();
    private String collectionId;
    private List<String> feedUrls;
    private String collectedAt;
    private int count;
    private List<ManifestItem> items;

    public static Manifest of(String collectionId, List<String> feeds, List<ArticleRecord> records) {
        List<ManifestItem> items = new ArrayList<>();
        for (ArticleRecord r : records) {
            ManifestItem it = ManifestItem.builder()
                    .articleId(r.item().articleId())
                    .guid(r.item().guid())
                    .link(r.item().link())
                    .finalUrl(r.item().link())
                    .title(r.item().title())
                    .author(r.item().author())
                    .pubDate(r.item().pubDate())
                    .description(r.item().description())
                    .images(r.item().images())
                    .crawl(CrawlInfo.builder()
                            .status(r.crawlStatus())
                            .contentType(r.contentType())
                            .charset(r.charset())
                            .crawledAt(Instant.now().toString())
                            .htmlObjectKey(r.htmlObjectKey())
                            .build())
                    .build();
            items.add(it);
        }

        return Manifest.builder()
                .collectionId(collectionId)
                .feedUrls(feeds)
                .collectedAt(Instant.now().toString())
                .count(records.size())
                .items(items)
                .build();
    }
}
