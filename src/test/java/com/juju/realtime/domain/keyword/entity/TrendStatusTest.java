package com.juju.realtime.domain.keyword.entity;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")

class TrendStatusTest {

    @Test
    void 트렌드_상태_enum_값_확인() {
        // Given & When & Then
        assertThat(TrendStatus.UP.getDescription()).isEqualTo("상승");
        assertThat(TrendStatus.DOWN.getDescription()).isEqualTo("하락");
        assertThat(TrendStatus.NEW.getDescription()).isEqualTo("신규");
        assertThat(TrendStatus.MAINTAIN.getDescription()).isEqualTo("유지");
    }

    @Test
    void 트렌드_상태_symbol_확인() {
        // Given & When & Then
        assertThat(TrendStatus.UP.getSymbol()).isEqualTo("▲");
        assertThat(TrendStatus.DOWN.getSymbol()).isEqualTo("▼");
        assertThat(TrendStatus.NEW.getSymbol()).isEqualTo("+");
        assertThat(TrendStatus.MAINTAIN.getSymbol()).isEqualTo("-");
    }
}
