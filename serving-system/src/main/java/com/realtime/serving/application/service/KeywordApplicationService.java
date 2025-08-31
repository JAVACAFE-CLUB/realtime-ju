package com.realtime.serving.application.service;

import com.realtime.commonlib.exception.BusinessException;
import com.realtime.commonlib.exception.ErrorCode;
import com.realtime.serving.domain.keyword.entity.Keyword;
import com.realtime.serving.domain.keyword.repository.KeywordRepository;
import com.realtime.serving.presentation.rest.keyword.dto.KeywordCreateRequest;
import com.realtime.serving.presentation.rest.keyword.dto.KeywordResponse;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class KeywordApplicationService {

    private final KeywordRepository keywordRepository;

    @Transactional(readOnly = true)
    public List<KeywordResponse> getTopKeywords(int limit) {
        List<Keyword> keywords = keywordRepository.findTopKeywordsByRankingAsc(limit);

        return keywords.stream()
                .map(KeywordResponse::from)
                .collect(Collectors.toList());
    }

    @Transactional
    public KeywordResponse createKeyword(KeywordCreateRequest request) {
        Keyword keyword = Keyword.create(
                request.keyword(),
                request.ranking(),
                request.trendStatus()
        );

        Keyword savedKeyword = keywordRepository.save(keyword);
        return KeywordResponse.from(savedKeyword);
    }

    @Transactional
    public void deleteKeyword(Long keywordId) {
        if (!keywordRepository.existsById(keywordId)) {
            throw new BusinessException(ErrorCode.KEYWORD_NOT_FOUND);
        }
        keywordRepository.deleteById(keywordId);
    }

}
