package com.realtime.refine.infrastructure.storage.minio;

public record MinioUri(String bucket, String objectKey) {
    public static MinioUri parse(String uri) {
        // [검증] 스킴 및 형식 유효성 검사
        if (uri == null || !uri.startsWith("minio://")) {
            throw new IllegalArgumentException("Invalid minio uri: " + uri);
        }
        String path = uri.substring("minio://".length());
        int slash = path.indexOf('/');
        if (slash <= 0 || slash >= path.length() - 1) {
            throw new IllegalArgumentException("Invalid minio uri path: " + uri);
        }
        // [파싱] 버킷/오브젝트 키 분리
        String bucket = path.substring(0, slash);
        String objectKey = path.substring(slash + 1);
        return new MinioUri(bucket, objectKey);
    }
}


