package com.realtime.common.util.time;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class TimeParser {

    private TimeParser() {}

    public static String toIsoUtc(Instant instant) {
        if (instant == null) return null;
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }

    public static String parseRfc822ToIsoUtc(String rfc822) {
        if (rfc822 == null || rfc822.isBlank()) return null;
        try {
            OffsetDateTime odt = OffsetDateTime.parse(rfc822, DateTimeFormatter.RFC_1123_DATE_TIME);
            return DateTimeFormatter.ISO_INSTANT.format(odt.toInstant());
        } catch (Exception e) {
            return rfc822; // 원문 반환
        }
    }

    public static String toIsoUtcFromLocal(OffsetDateTime local, ZoneId zone) {
        if (local == null) return null;
        return DateTimeFormatter.ISO_INSTANT.format(local.atZoneSameInstant(zone).toInstant());
    }
}


