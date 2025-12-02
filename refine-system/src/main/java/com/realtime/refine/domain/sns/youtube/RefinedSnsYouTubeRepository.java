package com.realtime.refine.domain.sns.youtube;

public interface RefinedSnsYouTubeRepository {

    RefinedSnsYouTube findById(String id);

    RefinedSnsYouTube findByContentId(String contentId);

    boolean save(RefinedSnsYouTube document);
}
