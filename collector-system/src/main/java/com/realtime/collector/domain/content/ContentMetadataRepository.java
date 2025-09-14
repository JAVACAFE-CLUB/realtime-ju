package com.realtime.collector.domain.content;

import java.util.List;


public interface ContentMetadataRepository {

    void saveAll(List<ContentMetadata> entities);
}
