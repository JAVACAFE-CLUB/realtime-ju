package com.realtime.collector.application.docs.wikipedia.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 수집된 위키 샤드들의 메타데이터를 담아 저장하는 매니페스트 객체.
 */
@Getter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class WikiManifest {

    private String collectionId;
    private String lang;
    private String dumpDate;
    private int pagesTotal;
    private int shardsTotal;
    private long bytesTotal;
    /** 업로드된 샤드 오브젝트 키 목록 */
    private List<String> shards;
    private String schemaVersion;
    private String createdAt;
}
