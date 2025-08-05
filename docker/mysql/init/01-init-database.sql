-- MySQL 문자셋 설정
SET GLOBAL log_bin_trust_function_creators = 1;
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;
SET character_set_connection=utf8mb4;

-- 환경변수로 전달된 데이터베이스 사용 (realtime_db)

CREATE TABLE IF NOT EXISTS keywords (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    keyword VARCHAR(100) NOT NULL COMMENT '검색어',
    ranking INT NOT NULL COMMENT '랭킹 (1-100)',
    trend_status VARCHAR(20) NOT NULL COMMENT '트렌드 상태 (UP, DOWN, NEW, MAINTAIN)',
    search_count BIGINT NOT NULL DEFAULT 0 COMMENT '검색 횟수',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    
    INDEX idx_keyword_ranking (ranking),
    INDEX idx_keyword_updated_at (updated_at),
    INDEX idx_keyword_keyword (keyword),
    
    UNIQUE KEY uk_keyword_ranking (ranking)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='실시간 검색어 테이블';

-- 초기 테스트 데이터 삽입
INSERT INTO keywords (keyword, ranking, trend_status, search_count) VALUES
('김치찌개', 1, 'MAINTAIN', 15420),
('떡볶이', 2, 'UP', 12350),
('삼겹살', 3, 'DOWN', 9876),
('치킨', 4, 'UP', 8765),
('라면', 5, 'NEW', 7654),
('피자', 6, 'UP', 6543),
('초밥', 7, 'NEW', 5432),
('파스타', 8, 'UP', 4321),
('햄버거', 9, 'UP', 3210),
('샌드위치', 10, 'UP', 2109)
ON DUPLICATE KEY UPDATE
    keyword = VALUES(keyword),
    trend_status = VALUES(trend_status),
    search_count = VALUES(search_count),
    updated_at = CURRENT_TIMESTAMP;
