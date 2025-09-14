package com.realtime.common.util;

import java.util.concurrent.ThreadLocalRandom;

public final class CollectionIdGenerator {

    private CollectionIdGenerator() {
    }

    public static String generateId(String prefix) {
        String safePrefix = prefix == null || prefix.isBlank() ? "GEN" : prefix.trim();
        long nowMillis = System.currentTimeMillis();
        int randomFourDigits = ThreadLocalRandom.current().nextInt(1000, 9999);
        return safePrefix + "_" + nowMillis + "_" + randomFourDigits;
    }
}


