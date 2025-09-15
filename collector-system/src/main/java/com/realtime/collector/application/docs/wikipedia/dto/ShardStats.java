package com.realtime.collector.application.docs.wikipedia.dto;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class ShardStats {
    private int pagesTotal;
    private int shardsTotal;
    private long bytesTotal;
    private String lang;
    private String dumpDate;
    private List<String> shardKeys = new ArrayList<>();
}
