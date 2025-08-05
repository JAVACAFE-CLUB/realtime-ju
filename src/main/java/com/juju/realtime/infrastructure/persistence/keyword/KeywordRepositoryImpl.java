package com.juju.realtime.infrastructure.persistence.keyword;

import com.juju.realtime.domain.keyword.entity.Keyword;
import com.juju.realtime.domain.keyword.repository.KeywordRepository;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class KeywordRepositoryImpl implements KeywordRepository {

    private final KeywordJpaRepository jpaRepository;
    private final KeywordMapper mapper;

    @Override
    public Keyword save(Keyword keyword) {
        KeywordEntity entity = mapper.toEntity(keyword);
        KeywordEntity savedEntity = jpaRepository.save(entity);

        return mapper.toDomain(savedEntity);
    }

    @Override
    public Optional<Keyword> findById(Long id) {
        return jpaRepository.findById(id)
                .map(mapper::toDomain);
    }

    @Override
    public List<Keyword> findTopKeywordsByRankingAsc(int limit) {
        PageRequest pageRequest = PageRequest.of(0, limit);
        List<KeywordEntity> entities = jpaRepository.findAllByOrderByRankingAsc(pageRequest);
        return mapper.toDomainList(entities);
    }

    @Override
    public void deleteById(Long id) {
        jpaRepository.deleteById(id);
    }
}
