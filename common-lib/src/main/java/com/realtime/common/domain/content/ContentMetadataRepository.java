package com.realtime.common.domain.content;

import java.util.List;

public interface ContentMetadataRepository {

    void saveAll(List<ContentMetadata> entities);

    void save(ContentMetadata metadata);
}

