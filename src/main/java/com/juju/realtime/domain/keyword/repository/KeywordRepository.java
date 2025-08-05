package com.juju.realtime.domain.keyword.repository;

import com.juju.realtime.domain.keyword.entity.Keyword;
import java.util.List;
import java.util.Optional;

public interface KeywordRepository {

    Keyword save(Keyword keyword);

    Optional<Keyword> findById(Long id);

    List<Keyword> findAllByOrderByRankingAsc();

    void deleteById(Long id);
}
