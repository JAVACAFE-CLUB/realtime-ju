package com.juju.realtime.application.service;

import com.juju.realtime.domain.keyword.entity.Keyword;
import com.juju.realtime.domain.keyword.repository.KeywordRepository;
import com.juju.realtime.global.exception.BusinessException;
import com.juju.realtime.global.exception.ErrorCode;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordCreateRequest;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordResponse;
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
        keywordRepository.findById(keywordId)
                .orElseThrow(() -> new BusinessException(ErrorCode.KEYWORD_NOT_FOUND));

        keywordRepository.deleteById(keywordId);
    }

}
