package com.juju.realtime.presentation.rest.keyword;

import com.juju.realtime.application.service.KeywordApplicationService;
import com.juju.realtime.global.common.ApiResponse;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordCreateRequest;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordResponse;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseStatus;

@RestController
@RequestMapping("/api/keywords")
@RequiredArgsConstructor
public class KeywordController {

    private final KeywordApplicationService keywordApplicationService;

    @GetMapping
    public ApiResponse<List<KeywordResponse>> getRealTimeKeywords(Integer limit) {
        List<KeywordResponse> keywords = keywordApplicationService.getTopKeywords(limit);
        return ApiResponse.success(keywords);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ApiResponse<KeywordResponse> createKeyword(@RequestBody KeywordCreateRequest request) {
        KeywordResponse response = keywordApplicationService.createKeyword(request);
        return ApiResponse.success(response);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteKeyword(@PathVariable Long id) {
        keywordApplicationService.deleteKeyword(id);
    }
}
