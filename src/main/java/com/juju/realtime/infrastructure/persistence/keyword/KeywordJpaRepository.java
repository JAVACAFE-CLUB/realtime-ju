package com.juju.realtime.infrastructure.persistence.keyword;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KeywordJpaRepository extends JpaRepository<KeywordEntity, Long> {

    List<KeywordEntity> findAllByOrderByRankingAsc();
}
