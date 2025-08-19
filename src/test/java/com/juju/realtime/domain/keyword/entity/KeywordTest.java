package com.juju.realtime.domain.keyword.entity;

import static org.assertj.core.api.Assertions.assertThat;

import com.juju.realtime.TestDataFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@DisplayName("🔥 Keyword 엔티티 테스트")
class KeywordTest {

    @Nested
    @DisplayName("검색어 엔티티 생성")
    class KeywordEntityCreationTests {

        @Test
        @DisplayName("✅ 검색어 엔티티 생성 성공")
        void 검색어_엔티티_생성_성공() {
            // Given
            String keyword = TestDataFactory.KIMCHI_JJIGAE;
            Integer ranking = 1;
            TrendStatus trendStatus = TrendStatus.MAINTAIN;

            // When
            Keyword searchKeyword = Keyword.builder()
                    .keyword(keyword)
                    .ranking(ranking)
                    .trendStatus(trendStatus)
                    .searchCount(0L)
                    .build();

            // Then
            assertThat(searchKeyword.getKeyword()).isEqualTo(keyword);
            assertThat(searchKeyword.getRanking()).isEqualTo(ranking);
            assertThat(searchKeyword.getTrendStatus()).isEqualTo(trendStatus);
            assertThat(searchKeyword.getSearchCount()).isEqualTo(0L);
        }

        @Test
        @DisplayName("✅ 검색어 엔티티 정적 생성 메서드 테스트")
        void 검색어_엔티티_정적_생성_메서드_테스트() {
            // Given
            String keyword = TestDataFactory.DOENJANG_JJIGAE;
            Integer ranking = 1;
            TrendStatus trendStatus = TrendStatus.UP;

            // When
            Keyword searchKeyword = Keyword.create(keyword, ranking, trendStatus);

            // Then
            assertThat(searchKeyword.getKeyword()).isEqualTo(keyword);
            assertThat(searchKeyword.getRanking()).isEqualTo(ranking);
            assertThat(searchKeyword.getTrendStatus()).isEqualTo(trendStatus);
            assertThat(searchKeyword.getSearchCount()).isEqualTo(0L);
            assertThat(searchKeyword.getCreatedAt()).isNotNull();
            assertThat(searchKeyword.getUpdatedAt()).isNotNull();
            assertThat(searchKeyword.getId()).isNull();
        }
    }
}
