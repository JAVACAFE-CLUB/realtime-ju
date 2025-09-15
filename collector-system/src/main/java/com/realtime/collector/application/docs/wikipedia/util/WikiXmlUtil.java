package com.realtime.collector.application.docs.wikipedia.util;

import javax.xml.stream.XMLStreamReader;

public final class WikiXmlUtil {

    private WikiXmlUtil() {}

    public static String getAttribute(XMLStreamReader reader, String localName) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            if (localName.equals(reader.getAttributeLocalName(i))) {
                return reader.getAttributeValue(i);
            }
        }
        return null;
    }

    public static Integer parseIntSafe(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return null; }
    }
}
