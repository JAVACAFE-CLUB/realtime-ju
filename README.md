# Realtime Search Platform

실시간 뉴스 및 소셜미디어 데이터를 수집하고 분석하여 트렌딩 키워드를 제공하는 플랫폼

## ☁️ 데이터 플로우

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ 수집 시스템    │ -> │ 정제 시스템     │ -> │ 색인 시스템    │ ->  │ 서빙 시스템    │
│(Collector)  │    │(Refine)      │    │(Index)       │    │ (Serving)    │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
```

## ☁️ 프로젝트 구조

- `common-lib`: 공통 라이브러리 (DTO, 설정, 유틸리티)
- `collector-system`: 데이터 수집 시스템 (뉴스, SNS 크롤링)
- `refine-system`: 데이터 정제 시스템
- `index-system`: 검색 색인 시스템 (Elasticsearch 색인)
- `serving-system`:  API 서빙 시스템 (REST API)

## ☁️ 기술 스택

- Core
    - Java 21
    - Spring Boot 3.3.1
    - Gradle 8.x
- 데이터 저장
    - MySQL 8.0
    - Elasticsearch 8.x - 실시간 검색 인덱스
    - Redis 7.x - 캐시 및 세션
- 메시징
    - Apache Kafka - 시스템 간 비동기 통신
- 운영 (예정)
    - Docker & Docker Compose - 컨테이너화
    - Kubernetes - 컨테이너 오케스트레이션
    - Jenkins - CI/CD 파이프라인

## ☁️ 설치 및 실행

### 1. 프로젝트 클론 및 환경 설정

```bash
# 프로젝트 클론
git clone <repository-url>
cd realtime-ju

# 환경 변수 설정
cp .env.example .env

# 필요에 따라 .env 파일의 환경 변수를 수정하세요
```

### 2. 인프라 실행

```bash
# MySQL만 실행
docker-compose up -d mysql

# 전체 인프라 실행 (Kafka, Elasticsearch, Redis 포함)
docker-compose --profile kafka --profile elasticsearch --profile redis up -d
```

### 3. 애플리케이션 빌드

```
# 전체 프로젝트 빌드
./gradlew build

# 공통 라이브러리를 로컬 Maven 저장소에 발행
./gradlew :common-lib:publishToMavenLocal
```

### 4. 시스템 실행

> 애플리케이션은 IDE에서 실행

```
# 서빙 시스템 실행
./gradlew :serving-system:bootRun

# 또는 IntelliJ에서 ServingApplication.java 실행
```

## ️☁️ 테스트 실행

```bash
# 특정 시스템만 빌드
./gradlew :common-lib:build
./gradlew :serving-system:build

# 특정 시스템만 테스트
./gradlew :common-lib:test
./gradlew :serving-system:test
```
