package com.realtime.refine.infrastructure.parser.tika;

import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TikaResult {
    String content;
    Map<String, String> metadata;
    String contentType;
    String charset;
    String language;
}



