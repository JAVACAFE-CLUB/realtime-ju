package com.realtime.serving.infrastructure.persistence.keyword;

import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KeywordJpaRepository extends JpaRepository<KeywordEntity, Long> {

    List<KeywordEntity> findAllByOrderByRankingAsc(Pageable pageable);
}
