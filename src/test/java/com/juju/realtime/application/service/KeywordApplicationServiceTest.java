package com.juju.realtime.application.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.verify;

import com.juju.realtime.TestAssertions;
import com.juju.realtime.TestDataFactory;
import com.juju.realtime.domain.keyword.entity.Keyword;
import com.juju.realtime.domain.keyword.entity.TrendStatus;
import com.juju.realtime.domain.keyword.repository.KeywordRepository;
import com.juju.realtime.global.exception.BusinessException;
import com.juju.realtime.global.exception.ErrorCode;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordCreateRequest;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordResponse;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ExtendWith(MockitoExtension.class)
@ActiveProfiles("test")
@DisplayName("🔥 KeywordApplicationService 테스트")
class KeywordApplicationServiceTest {

    @Mock
    private KeywordRepository keywordRepository;

    @InjectMocks
    private KeywordApplicationService keywordApplicationService;

    private Keyword sampleKeyword;
    private KeywordCreateRequest createRequest;

    @BeforeEach
    void setUp() {
        sampleKeyword = TestDataFactory.createSampleKeyword();
        createRequest = TestDataFactory.createSampleRequest();
    }

    @Nested
    @DisplayName("검색어 조회")
    class KeywordRetrievalTests {

        @Test
        @DisplayName("✅ 실시간 검색어 TOP 10 조회 - 성공")
        void 실시간_검색어_TOP10_조회_성공() {
            // given
            List<Keyword> top3Keywords = TestDataFactory.createTop3Keywords();
            given(keywordRepository.findAllByOrderByRankingAsc())
                    .willReturn(top3Keywords);

            // when
            List<KeywordResponse> responses = keywordApplicationService.getTopKeywords(10);

            // then
            TestAssertions.assertKeywordResponseList(responses, 3);
            TestAssertions.assertFirstKeyword(responses,
                    TestDataFactory.KIMCHI_JJIGAE, 1, TrendStatus.UP);
            verify(keywordRepository).findAllByOrderByRankingAsc();
        }

        @Test
        @DisplayName("✅ 실시간 검색어 TOP 5 조회 - limit 적용")
        void 실시간_검색어_TOP5_조회_limit적용_성공() {
            // given
            List<Keyword> top5Keywords = TestDataFactory.createTop5Keywords();
            given(keywordRepository.findAllByOrderByRankingAsc())
                    .willReturn(top5Keywords);

            // when
            List<KeywordResponse> responses = keywordApplicationService.getTopKeywords(3);

            // then
            TestAssertions.assertKeywordResponseList(responses, 3);
            TestAssertions.assertKeywordOrder(responses,
                    TestDataFactory.KIMCHI_JJIGAE,
                    TestDataFactory.DOENJANG_JJIGAE,
                    TestDataFactory.SUNDUBU_JJIGAE);
            verify(keywordRepository).findAllByOrderByRankingAsc();
        }
    }

    @Nested
    @DisplayName("검색어 생성")
    class KeywordCreationTests {

        @Test
        @DisplayName("✅ 검색어 등록 - 성공")
        void 검색어_등록_성공() {
            // given
            given(keywordRepository.save(any(Keyword.class)))
                    .willReturn(sampleKeyword);

            // when
            KeywordResponse response = keywordApplicationService.createKeyword(createRequest);

            // then
            TestAssertions.assertKeywordResponse(response,
                    sampleKeyword.getKeyword(),
                    sampleKeyword.getRanking(),
                    sampleKeyword.getTrendStatus());
            verify(keywordRepository).save(any(Keyword.class));
        }
    }

    @Nested
    @DisplayName("검색어 삭제")
    class KeywordDeletionTests {

        @Test
        @DisplayName("✅ 검색어 삭제 - 성공")
        void 검색어_삭제_성공() {
            // given
            Long keywordId = 1L;
            given(keywordRepository.findById(keywordId))
                    .willReturn(Optional.of(sampleKeyword));

            // when
            keywordApplicationService.deleteKeyword(keywordId);

            // then
            verify(keywordRepository).deleteById(keywordId);
        }

        @Test
        @DisplayName("❌ 검색어 삭제 - 존재하지 않는 검색어")
        void 검색어_삭제_존재하지않는검색어_예외발생() {
            // given
            Long keywordId = 999L;
            given(keywordRepository.findById(keywordId))
                    .willReturn(Optional.empty());

            // when & then
            assertThatThrownBy(() -> keywordApplicationService.deleteKeyword(keywordId))
                    .isInstanceOf(BusinessException.class)
                    .hasMessage(ErrorCode.KEYWORD_NOT_FOUND.getMessage());
        }
    }
}
