package com.juju.realtime.interfaces.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.verify;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.juju.realtime.TestDataFactory;
import com.juju.realtime.application.service.KeywordApplicationService;
import com.juju.realtime.global.exception.BusinessException;
import com.juju.realtime.global.exception.ErrorCode;
import com.juju.realtime.presentation.rest.keyword.KeywordController;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordCreateRequest;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordResponse;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(KeywordController.class)
@DisplayName("🔥 Keyword Controller TDD 테스트")
class KeywordControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KeywordApplicationService keywordService;

    private KeywordResponse sampleResponse;
    private KeywordCreateRequest createRequest;

    @BeforeEach
    void setUp() {
        sampleResponse = TestDataFactory.createSampleResponse();
        createRequest = TestDataFactory.createSampleRequest();
    }

    @Nested
    @DisplayName("검색어 조회 API")
    class KeywordRetrievalApiTests {

        @Test
        @DisplayName("✅ GET /api/keywords - 실시간 검색어 TOP 10 조회")
        void 실시간_검색어_TOP10_조회_API_성공() throws Exception {
            // given
            List<KeywordResponse> responses = TestDataFactory.createTop3KeywordResponses();
            given(keywordService.getTopKeywords(10)).willReturn(responses);

            // when & then
            mockMvc.perform(get("/api/keywords?limit=10")
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data").isArray())
                    .andExpect(jsonPath("$.data.length()").value(3))
                    .andExpect(jsonPath("$.data[0].keyword").value(TestDataFactory.KIMCHI_JJIGAE))
                    .andExpect(jsonPath("$.data[0].ranking").value(1))
                    .andExpect(jsonPath("$.data[0].trendStatus").value("UP"))
                    .andDo(print());

            verify(keywordService).getTopKeywords(10);
        }

        @Test
        @DisplayName("✅ GET /api/keywords?limit=5 - 실시간 검색어 TOP N 조회")
        void 실시간_검색어_TOP5_조회_API_성공() throws Exception {
            // given
            List<KeywordResponse> responses = List.of(sampleResponse);
            given(keywordService.getTopKeywords(5)).willReturn(responses);

            // when & then
            mockMvc.perform(get("/api/keywords")
                            .param("limit", "5")
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data").isArray())
                    .andExpect(jsonPath("$.data.length()").value(1))
                    .andDo(print());

            verify(keywordService).getTopKeywords(5);
        }
    }

    @Nested
    @DisplayName("검색어 생성 API")
    class KeywordCreationApiTests {

        @Test
        @DisplayName("✅ POST /api/keywords - 검색어 등록")
        void 검색어_등록_API_성공() throws Exception {
            // given
            given(keywordService.createKeyword(any(KeywordCreateRequest.class)))
                    .willReturn(sampleResponse);

            // when & then
            mockMvc.perform(post("/api/keywords")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(createRequest)))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.data.keyword").value(sampleResponse.keyword()))
                    .andExpect(jsonPath("$.data.ranking").value(sampleResponse.ranking()))
                    .andDo(print());

            verify(keywordService).createKeyword(any(KeywordCreateRequest.class));
        }
    }

    @Nested
    @DisplayName("검색어 삭제 API")
    class KeywordDeletionApiTests {

        @Test
        @DisplayName("✅ DELETE /api/keywords/{id} - 검색어 삭제")
        void 검색어_삭제_API_성공() throws Exception {
            // given
            Long keywordId = 1L;
            willDoNothing().given(keywordService).deleteKeyword(keywordId);

            // when & then
            mockMvc.perform(delete("/api/keywords/{id}", keywordId)
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isNoContent())
                    .andDo(print());

            verify(keywordService).deleteKeyword(keywordId);
        }

        @Test
        @DisplayName("❌ DELETE /api/keywords/{id} - 존재하지 않는 키워드")
        void 검색어_삭제_API_존재하지않는키워드_404에러() throws Exception {
            // given
            Long keywordId = 999L;
            willThrow(new BusinessException(ErrorCode.KEYWORD_NOT_FOUND))
                    .given(keywordService).deleteKeyword(keywordId);

            // when & then
            mockMvc.perform(delete("/api/keywords/{id}", keywordId)
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.message").value(ErrorCode.KEYWORD_NOT_FOUND.getMessage()))
                    .andDo(print());
        }
    }
}
