package com.realtime.refine.domain.news.yna;

public interface RefinedNewsYnaRepository {

    RefinedNewsYna findById(String id);

    RefinedNewsYna findByContentId(String contentId);

    boolean save(RefinedNewsYna document);
}



