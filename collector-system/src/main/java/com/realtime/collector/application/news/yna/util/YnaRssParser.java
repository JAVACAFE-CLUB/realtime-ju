package com.realtime.collector.application.news.yna.util;

import com.realtime.collector.application.news.yna.dto.RssItem;
import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndEnclosure;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public final class YnaRssParser {

    private YnaRssParser() {
    }

    public static List<RssItem> parse(String rssXml) {
        try {
            SyndFeedInput input = new SyndFeedInput();
            SyndFeed feed = input.build(
                    new XmlReader(new ByteArrayInputStream(rssXml.getBytes(StandardCharsets.UTF_8))));
            
            List<RssItem> results = new ArrayList<>();
            for (SyndEntry entry : feed.getEntries()) {
                String link = entry.getLink();
                String guid = entry.getUri() != null ? entry.getUri() : link;
                String title = safeText(entry.getTitle());
                String author = safeText(entry.getAuthor());

                String description = null;
                SyndContent desc = entry.getDescription();
                if (desc != null && desc.getValue() != null) {
                    description = extractCdataText(desc.getValue());
                }

                String pubDate = entry.getPublishedDate() != null ? entry.getPublishedDate().toString() : null;

                List<String> images = new ArrayList<>();
                if (entry.getEnclosures() != null) {
                    for (SyndEnclosure enc : entry.getEnclosures()) {
                        if (enc.getType() != null && enc.getType().startsWith("image/")) {
                            images.add(enc.getUrl());
                        }
                    }
                }

                String articleId = deriveArticleId(guid, link);
                if (articleId != null && link != null) {
                    results.add(new RssItem(articleId, guid, link, title, author, pubDate, description, images));
                }
            }
            return results;
        } catch (Exception e) {
            return fallbackParse(rssXml); // Rome 파싱 실패 시 간단 폴백(정규식 기반)
        }
    }

    private static List<RssItem> fallbackParse(String rssXml) {
        List<RssItem> list = new ArrayList<>();
        Matcher m = Pattern.compile("<item>([\\s\\S]*?)</item>").matcher(rssXml);
        while (m.find()) {
            String item = m.group(1);
            String link = extractTag(item, "link");
            String guid = extractTag(item, "guid");
            String title = extractCdataText(extractTagRaw(item, "title"));
            String description = extractCdataText(extractTagRaw(item, "description"));
            String pubDate = extractTag(item, "pubDate");
            String author = extractTagNs(item, "dc:creator");
            String articleId = deriveArticleId(guid, link);
            List<String> images = extractMediaContent(item);
            if (articleId != null && link != null) {
                list.add(new RssItem(articleId, guid, link, title, author, pubDate, description, images));
            }
        }
        return list;
    }

    private static String extractCdataText(String html) {
        if (html == null) {
            return null;
        }
        Document doc = Jsoup.parseBodyFragment(html);
        return doc.text();
    }

    private static String safeText(String s) {
        return s == null ? null : s;
    }

    private static String deriveArticleId(String guid, String link) {
        String base = guid != null && !guid.isBlank() ? guid : link;
        if (base == null) {
            return null;
        }
        Matcher m = Pattern.compile("AKR\\d+").matcher(base);
        if (m.find()) {
            return m.group();
        }
        return null;
    }

    private static String extractTag(String xml, String tag) {
        String v = extractTagRaw(xml, tag);
        return v == null ? null : v.replaceAll("<[^>]+>", "").trim();
    }

    private static String extractTagNs(String xml, String tag) {
        Matcher m = Pattern.compile("<" + Pattern.quote(tag) + ">([\\s\\S]*?)</" + Pattern.quote(tag) + ">")
                .matcher(xml);
        if (m.find()) {
            return m.group(1).replaceAll("<[^>]+>", "").trim();
        }
        return null;
    }

    private static String extractTagRaw(String xml, String tag) {
        Matcher m = Pattern.compile("<" + Pattern.quote(tag) + ">([\\s\\S]*?)</" + Pattern.quote(tag) + ">")
                .matcher(xml);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private static List<String> extractMediaContent(String itemXml) {
        List<String> images = new ArrayList<>();
        Matcher m = Pattern.compile("<media:content[^>]*url=\\\"([^\\\"]+)\\\"").matcher(itemXml);
        while (m.find()) {
            images.add(m.group(1));
        }
        return images;
    }
}
