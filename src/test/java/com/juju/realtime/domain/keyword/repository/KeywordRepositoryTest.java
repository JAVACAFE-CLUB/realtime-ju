package com.juju.realtime.domain.keyword.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.juju.realtime.TestDataFactory;
import com.juju.realtime.domain.keyword.entity.Keyword;
import com.juju.realtime.domain.keyword.entity.TrendStatus;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
@DisplayName("🔥 KeywordRepository 테스트")
class KeywordRepositoryTest {

    @Autowired
    private KeywordRepository keywordRepository;

    private Keyword testKeyword1;
    private Keyword testKeyword2;

    @BeforeEach
    void setUp() {
        testKeyword1 = TestDataFactory.createKeyword(
                TestDataFactory.KIMCHI_JJIGAE, 1, TrendStatus.UP);
        testKeyword2 = TestDataFactory.createKeyword(
                TestDataFactory.DOENJANG_JJIGAE, 2, TrendStatus.MAINTAIN);
    }

    @Nested
    @DisplayName("검색어 저장 및 조회")
    class KeywordSaveAndRetrieveTests {

        @Test
        @DisplayName("✅ 검색어 저장 및 조회 성공")
        void 검색어_저장_및_조회_성공() {
            // Given
            Keyword savedKeyword = keywordRepository.save(testKeyword1);

            // When
            Optional<Keyword> foundKeyword = keywordRepository.findById(savedKeyword.getId());

            // Then
            assertThat(foundKeyword).isPresent();
            assertThat(foundKeyword.get().getKeyword()).isEqualTo(TestDataFactory.KIMCHI_JJIGAE);
            assertThat(foundKeyword.get().getRanking()).isEqualTo(1);
            assertThat(foundKeyword.get().getTrendStatus()).isEqualTo(TrendStatus.UP);
            assertThat(foundKeyword.get().getSearchCount()).isEqualTo(1000L);
        }
    }

    @Nested
    @DisplayName("검색어 정렬 조회")
    class KeywordSortingTests {


        @Test
        @DisplayName("✅ 랭킹순으로 검색어 TOP N 조회 성공")
        void 랭킹순으로_검색어_TOP_N_조회_성공() {
            // Given
            Keyword keyword3 = TestDataFactory.createKeyword(
                    TestDataFactory.SUNDUBU_JJIGAE, 3, TrendStatus.DOWN);
            Keyword keyword4 = TestDataFactory.createKeyword(
                    "된장찌개", 4, TrendStatus.UP);
            Keyword keyword5 = TestDataFactory.createKeyword(
                    "순두부찌개", 5, TrendStatus.MAINTAIN);

            keywordRepository.save(testKeyword2); // ranking 2
            keywordRepository.save(testKeyword1); // ranking 1  
            keywordRepository.save(keyword3);     // ranking 3
            keywordRepository.save(keyword4);     // ranking 4
            keywordRepository.save(keyword5);     // ranking 5

            // When
            List<Keyword> keywords = keywordRepository.findTopKeywordsByRankingAsc(3);

            // Then
            assertThat(keywords).hasSize(3);
            assertThat(keywords.get(0).getRanking()).isEqualTo(1);
            assertThat(keywords.get(1).getRanking()).isEqualTo(2);
            assertThat(keywords.get(2).getRanking()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("검색어 삭제")
    class KeywordDeletionTests {

        @Test
        @DisplayName("✅ 검색어 삭제 성공")
        void 검색어_삭제_성공() {
            // Given
            Keyword savedKeyword = keywordRepository.save(testKeyword1);
            assertThat(keywordRepository.existsById(savedKeyword.getId())).isTrue();

            // When
            keywordRepository.deleteById(savedKeyword.getId());

            // Then
            assertThat(keywordRepository.existsById(savedKeyword.getId())).isFalse();
        }
    }
}
