package com.realtime.serving.domain.keyword.repository;

import com.realtime.serving.domain.keyword.entity.Keyword;
import java.util.List;
import java.util.Optional;

public interface KeywordRepository {

    Keyword save(Keyword keyword);

    Optional<Keyword> findById(Long id);

    boolean existsById(Long id);

    List<Keyword> findTopKeywordsByRankingAsc(int limit);

    void deleteById(Long id);
}
