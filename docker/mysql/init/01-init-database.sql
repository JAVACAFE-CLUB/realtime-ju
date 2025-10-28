-- MySQL 문자셋 설정
SET GLOBAL log_bin_trust_function_creators = 1;
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;
SET character_set_connection=utf8mb4;

-- 환경변수로 전달된 데이터베이스 사용 (realtime_db)

-- ========================================
-- 콘텐츠 메타데이터 테이블 생성
-- 수집된 원본 데이터의 메타정보를 저장
-- ========================================
CREATE TABLE IF NOT EXISTS content_metadata (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '기본키 (자동증가)',
  source         VARCHAR(20)   NOT NULL COMMENT '데이터 소스 (YOUTUBE/WIKI/NEWS)',
  external_id    VARCHAR(128)  NOT NULL COMMENT '외부 시스템 고유 ID (YouTube 비디오 ID 등)',
  title          VARCHAR(500)  NOT NULL COMMENT '콘텐츠 제목',
  raw_uri        VARCHAR(1024) NOT NULL COMMENT 'MinIO 원본 데이터 저장 경로 (minio://bucket/path)',
  refined_id     VARCHAR(24)   NULL COMMENT '정제된 데이터 ID (MongoDB ObjectId 등)',
  collection_id  VARCHAR(40)   NULL COMMENT '수집 배치 식별자 (선택사항)',
  collected_at   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 수집 시각',
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='수집된 콘텐츠의 메타데이터 저장 테이블';

-- ========================================
-- 인덱스 생성 (성능 최적화)
-- ========================================

-- 소스별, 외부ID별, 수집시간 역순 조회용 복합 인덱스
CREATE INDEX idx_cm_source_ext_collected
  ON content_metadata (source, external_id, collected_at DESC)
  COMMENT '소스별 최신 데이터 조회 최적화';

-- 수집 시간별 조회용 인덱스 (시계열 분석)
CREATE INDEX idx_cm_collected_at
  ON content_metadata (collected_at)
  COMMENT '시간대별 수집 데이터 조회 최적화';

-- 배치 ID별 조회용 인덱스
CREATE INDEX idx_cm_collection_id
  ON content_metadata (collection_id)
  COMMENT '배치별 수집 데이터 조회 최적화';

-- 정제된 데이터 ID 조회용 인덱스
CREATE INDEX idx_cm_refined_id
  ON content_metadata (refined_id)
  COMMENT '정제 데이터 연결 조회 최적화';


-- 서빙 시스템 활용

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
