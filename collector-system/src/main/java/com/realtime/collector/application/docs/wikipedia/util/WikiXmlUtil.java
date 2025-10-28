package com.realtime.collector.application.docs.wikipedia.util;

import java.nio.file.Path;
import java.util.Optional;
import javax.xml.stream.XMLStreamReader;

/**
 * Wikipedia XML 파싱 과정에서 자주 쓰이는 보조 유틸리티 모음입니다.
 */
public final class WikiXmlUtil {

    private static final String BZIP2_EXTENSION = ".bz2";
    private static final String BZIP_EXTENSION = ".bz";

    private WikiXmlUtil() {
    }

    /**
     * 현재 요소에서 지정한 로컬 이름의 속성 값을 반환합니다. 없으면 null.
     */
    public static String getAttribute(XMLStreamReader reader, String localName) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            if (localName.equals(reader.getAttributeLocalName(i))) {
                return reader.getAttributeValue(i);
            }
        }
        return null;
    }

    /**
     * 공백을 허용하며, 파싱 실패 시 null을 반환하는 안전한 정수 변환.
     */
    public static Integer parseIntSafe(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 파일명만 추출하여 문자열로 반환합니다.
     */
    public static String getFileName(Path path) {
        return Optional.ofNullable(path.getFileName())
                .map(Path::toString)
                .orElse("");
    }

    /**
     * bzip/bzip2 확장자를 기준으로 압축 여부를 판단합니다.
     */
    public static boolean isCompressed(String fileName) {
        return fileName.endsWith(BZIP2_EXTENSION) || fileName.endsWith(BZIP_EXTENSION);
    }
}
