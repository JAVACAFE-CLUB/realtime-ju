package com.realtime.common.util.html;

import java.util.Set;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Safelist;

public final class HtmlCleaner {

    private static final Safelist DEFAULT_SAFELIST = Safelist.relaxed()
            .addTags("img", "figure", "figcaption")
            .addAttributes(":all", "class", "id")
            .addAttributes("img", "src", "alt", "title");

    private HtmlCleaner() {}

    public static String toPlainText(String html) {
        if (html == null) return null;
        Document doc = Jsoup.parse(html);
        removeNoise(doc);
        return doc.text();
    }

    public static String sanitize(String html, Safelist safelist) {
        if (html == null) return null;
        return Jsoup.clean(html, safelist != null ? safelist : DEFAULT_SAFELIST);
    }

    public static void removeNoise(Document doc) {
        if (doc == null) return;
        Set<String> selectors = Set.of(
                "script", "style", "iframe", "yna-ad-script",
                "aside[class^=aside-box]", 
                "div[id^=google_ads_iframe_]",
                ".ads-article01", ".ads-box", ".related-zone", ".rel"
        );
        for (String sel : selectors) {
            for (Element el : doc.select(sel)) {
                el.remove();
            }
        }
    }
}


