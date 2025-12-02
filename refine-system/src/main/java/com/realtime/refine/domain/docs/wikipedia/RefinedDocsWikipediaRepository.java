package com.realtime.refine.domain.docs.wikipedia;

import java.util.List;
import java.util.Map;

public interface RefinedDocsWikipediaRepository {

    RefinedDocsWikipedia findById(String id);

    RefinedDocsWikipedia findByPageId(String pageId);

    boolean save(RefinedDocsWikipedia document);

    /**
     * 여러 pageId로 배치 조회
     */
    Map<String, RefinedDocsWikipedia> findByPageIds(List<String> pageIds);

    /**
     * 배치 저장
     */
    boolean saveAll(List<RefinedDocsWikipedia> documents);
}

