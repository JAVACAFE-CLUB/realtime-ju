package com.realtime.common.util.net;

import java.net.URI;
import java.net.URISyntaxException;

public final class UrlNormalizer {

    private UrlNormalizer() {}

    public static String normalize(String url) {
        if (url == null) return null;
        try {
            URI u = new URI(url);
            String path = u.getPath() != null ? u.getPath() : "";
            String normalizedPath = path.replaceAll("//+", "/");
            return new URI(u.getScheme(), u.getAuthority(), normalizedPath, u.getQuery(), u.getFragment()).toString();
        } catch (URISyntaxException e) {
            return url;
        }
    }
}


