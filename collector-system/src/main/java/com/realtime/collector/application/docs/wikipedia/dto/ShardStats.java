package com.realtime.collector.application.docs.wikipedia.dto;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 수집/업로드 결과에 대한 통계 정보.
 */
@Getter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ShardStats {
    
    private int pagesTotal;
    private int shardsTotal;
    private long bytesTotal;
    private String lang;
    private String dumpDate;
    @Builder.Default
    private List<String> shardKeys = new ArrayList<>();
}
