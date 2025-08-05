package com.juju.realtime.infrastructure.persistence.keyword;

import com.juju.realtime.TestDataFactory;
import com.juju.realtime.domain.keyword.entity.TrendStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
@DisplayName("🔥 KeywordJpaRepository 테스트")
class KeywordJpaRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private KeywordJpaRepository keywordJpaRepository;

    @BeforeEach
    void setUp() {
        // 테스트 데이터 초기화
        entityManager.clear();
    }

    @Nested
    @DisplayName("검색어 저장 및 조회")
    class KeywordSaveAndRetrieveTests {

        @Test
        @DisplayName("✅ 키워드 저장 및 조회 성공")
        void 키워드_저장_및_조회_성공() {
            // Given
            KeywordEntity keywordEntity = createAndSaveKeywordEntity(
                    TestDataFactory.KIMCHI_JJIGAE, 1, TrendStatus.MAINTAIN);

            // When
            Optional<KeywordEntity> result = keywordJpaRepository.findById(keywordEntity.getId());

            // Then
            assertThat(result).isPresent();
            assertThat(result.get().getKeyword()).isEqualTo(TestDataFactory.KIMCHI_JJIGAE);
            assertThat(result.get().getRanking()).isEqualTo(1);
            assertThat(result.get().getTrendStatus()).isEqualTo(TrendStatus.MAINTAIN);
        }
    }

    @Nested
    @DisplayName("검색어 정렬 조회")
    class KeywordSortingTests {

        @Test
        @DisplayName("✅ 랭킹순으로 정렬 조회 성공")
        void 랭킹순으로_정렬_조회_성공() {
            // Given
            createAndSaveKeywordEntity(TestDataFactory.SUNDUBU_JJIGAE, 3, TrendStatus.DOWN);
            createAndSaveKeywordEntity(TestDataFactory.KIMCHI_JJIGAE, 1, TrendStatus.UP);
            createAndSaveKeywordEntity(TestDataFactory.DOENJANG_JJIGAE, 2, TrendStatus.MAINTAIN);

            // When
            List<KeywordEntity> results = keywordJpaRepository.findAllByOrderByRankingAsc();

            // Then
            assertThat(results).hasSize(3);
            assertThat(results.get(0).getRanking()).isEqualTo(1);
            assertThat(results.get(1).getRanking()).isEqualTo(2);
            assertThat(results.get(2).getRanking()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("검색어 삭제")
    class KeywordDeletionTests {

        @Test
        @DisplayName("✅ 키워드 삭제 성공")
        void 키워드_삭제_성공() {
            // Given
            KeywordEntity keywordEntity = createAndSaveKeywordEntity(
                    TestDataFactory.BUDAE_JJIGAE, 1, TrendStatus.UP);
            assertThat(keywordJpaRepository.findById(keywordEntity.getId())).isPresent();

            // When
            keywordJpaRepository.deleteById(keywordEntity.getId());

            // Then
            assertThat(keywordJpaRepository.findById(keywordEntity.getId())).isEmpty();
        }
    }

    private KeywordEntity createAndSaveKeywordEntity(String keyword, Integer ranking, TrendStatus trendStatus) {
        return createAndSaveKeywordEntityWithSearchCount(keyword, ranking, trendStatus, 0L);
    }

    private KeywordEntity createAndSaveKeywordEntityWithSearchCount(String keyword, Integer ranking, 
                                                                   TrendStatus trendStatus, Long searchCount) {
        KeywordEntity keywordEntity = KeywordEntity.builder()
                .keyword(keyword)
                .ranking(ranking)
                .trendStatus(trendStatus)
                .searchCount(searchCount)
                .build();

        entityManager.persistAndFlush(keywordEntity);
        return keywordEntity;
    }
}
