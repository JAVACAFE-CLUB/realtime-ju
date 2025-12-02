# Realtime Search Platform

실시간 뉴스 및 소셜미디어 데이터를 수집, 정제, 색인화하여 트렌딩 키워드를 제공하는 플랫폼

---

## 개요

이 플랫폼은 다양한 소스(Wikipedia, YNA 뉴스, YouTube)에서 실시간으로 데이터를 수집하고, 이를 정제 및 색인화하여 트렌딩 키워드를 추출하고 제공합니다.

### 주요 기능

- **실시간 데이터 수집**: Wikipedia, 연합뉴스(YNA), YouTube에서 데이터 수집
- **자동 정제**: HTML/JSON/Wikitext 파싱 및 텍스트 추출
- **한국어 키워드 추출**: Elasticsearch Nori 플러그인 기반 형태소 분석
- **가중치 기반 랭킹**: 소스별, 시간별 가중치 적용한 키워드 점수 계산
- **실시간 API**: 트렌딩 키워드 조회 REST API 제공

---

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           Realtime Search Platform Architecture                         │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│   ┌───────────────┐      ┌───────────────┐      ┌───────────────┐      ┌───────────────┐│
│   │   Collector   │      │    Refine     │      │     Index     │      │    Serving    ││
│   │    System     │─────>│    System     │─────>│    System     │─────>│    System     ││
│   │   (9080)      │      │   (9082)      │      │   (9083)      │      │   (9086)      ││
│   └───────┬───────┘      └───────┬───────┘      └───────┬───────┘      └───────┬───────┘│
│           │                      │                      │                      │        │
│           │                      │                      │                      │        │
│   ┌───────▼───────┐      ┌───────▼───────┐      ┌───────▼───────┐      ┌───────▼───────┐│
│   │     MinIO     │      │    MongoDB    │      │ Elasticsearch │      │     Redis     ││
│   │  (원본 저장)    │      │  (정제 저장)    │      │   (색인 저장)    │      │    (캐싱)      ││
│   └───────────────┘      └───────────────┘      └───────────────┘      └───────────────┘│
│                                                                                         │
│   ┌────────────────────────────────────────────────────────────────────────────────────┐│
│   │                              Apache Kafka (메시징 백본)                               ││
│   │  raw.* topics ──────────────────> refined.* topics ─────────────────> API Response ││
│   └────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                         │
│   ┌────────────────────────────────────────────────────────────────────────────────────┐│
│   │                               MySQL (메타데이터 저장)                                  ││
│   └────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 계층별 역할

| 계층     | 시스템       | 역할                | 저장소           |
|--------|-----------|-------------------|---------------|
| **수집** | Collector | 외부 소스에서 원본 데이터 수집 | MinIO         |
| **정제** | Refine    | HTML/텍스트 파싱 및 정제  | MongoDB       |
| **색인** | Index     | 키워드 추출 및 검색 색인    | Elasticsearch |
| **서빙** | Serving   | API 제공 및 랭킹 계산    | Redis + MySQL |

---

## Kafka 토픽 구조

| 토픽                       | 소스        | 설명                        |
|--------------------------|-----------|---------------------------|
| `raw.docs.wikipedia`     | Collector | Wikipedia 원본 데이터          |
| `raw.news.yna`           | Collector | YNA 뉴스 원본 데이터             |
| `raw.sns.youtube`        | Collector | YouTube 원본 데이터            |
| `refined.docs.wikipedia` | Refine    | Wikipedia 정제 데이터          |
| `refined.news.yna`       | Refine    | YNA 뉴스 정제 데이터             |
| `refined.sns.youtube`    | Refine    | YouTube 정제 데이터            |
| `*.dlq`                  | 각 시스템     | Dead Letter Queue (오류 처리) |


### 메시지 구조

**CollectMessage (수집 → 정제)**

```json
{
  "schemaVersion": "1",
  "collectionId": "YNA_20250902_001",
  "source": "NEWS_YNA",
  "occurredAt": "2025-09-02T10:30:00Z",
  "rawDataUrl": "minio://raw-news-yna/2025-09-02/article_123.html",
  "recordCount": 1
}
```

**RefineMessage (정제 → 색인)**

```json
{
  "schemaVersion": "1",
  "collectionId": "YNA_123",
  "source": "NEWS_YNA",
  "occurredAt": "2025-09-02T10:35:00Z",
  "refinedId": "refined_abc123def456"
}
```

---

## 1. 수집 시스템 (Collector System)

> **포트**: 9080 | **역할**: 외부 소스에서 데이터 수집 → Kafka 메시지 발송

### 요구사항 충족 평가

| 시스템    | 요구사항              | 상태              | 구현 방식                        |
|--------|-------------------|-----------------|------------------------------|
| **수집** | 대용량 파일 처리 (1GB+)  | ✅ 충족            | StAX 스트리밍 + BZip2 압축 해제      |
| **수집** | 메시지 분할 (1MB 이하)   | ✅ 충족            | NDJSON + GZIP 샤드 (500페이지/샤드) |
| **수집** | HTML 크롤링 (하루치 수집) | ✅ 충족            | RSS 피드 기반 병렬 크롤링             |
| **수집** | RSS 활용            | ✅ 충족            | Rome RSS Parser + 폴백 파싱      |
| **수집** | YouTube API 연동    | ✅ 충족            | YouTube Data API v3          |
| **수집** | API 호출 장애 대응      | ✅ 충족            | Exponential Backoff + 재시도    |

### 1.1 대용량 파일 처리 (Wikipedia)

**요구사항**: 1GB 이상 파일 핸들링, 메시지 1MB 이하로 분할

**구현 방식**:

```
Wikipedia XML 덤프 (1GB+)
    ↓ BZip2 압축 해제 (스트리밍)
    ↓ StAX 파서 (XMLStreamReader)
    ↓ 500페이지 단위 샤드 분할
    ↓ NDJSON + GZIP 압축 (약 500KB~1MB)
    ↓ MinIO 업로드 + Kafka 이벤트 발행
```

**설정값**:

| 설정                | 값      | 설명         |
|-------------------|--------|------------|
| `pages-per-shard` | 500    | 샤드당 페이지 수  |
| 압축 방식             | GZIP   | 샤드 압축      |
| 형식                | NDJSON | 스트리밍 파싱 용이 |

#### 💡 왜 500 페이지로 샤드 했는가?

- **Kafka 메시지 크기 제한**: Kafka 기본 최대 메시지 크기는 1MB, 500페이지 × GZIP 압축 ≈ 500KB~1MB로 적정
- **메모리 효율**: 한 번에 너무 많은 페이지를 메모리에 올리면 OOM 위험
- **실패 복구 단위**: 샤드 처리 실패 시 500페이지만 재처리하면 됨 (전체 재처리 방지)
- **병렬 처리 효율**: 샤드가 너무 크면 Consumer 간 부하 불균형, 너무 작으면 오버헤드 증가

#### 💡 NDJSON 형식이란?

- **Newline Delimited JSON**: 한 줄에 하나의 JSON 객체, 줄바꿈(`\n`)으로 구분
- **스트리밍 파싱 가능**: 전체 파일을 메모리에 올리지 않고 한 줄씩 읽어서 처리
- **일반 JSON 배열과의 차이**:
  ```
  # 일반 JSON 배열 (전체를 파싱해야 함)
  [{"id": 1}, {"id": 2}, {"id": 3}]

  # NDJSON (한 줄씩 독립적으로 파싱 가능)
  {"id": 1}
  {"id": 2}
  {"id": 3}
  ```
- **대용량 처리에 적합**: Wikipedia 덤프처럼 수십만 개의 문서를 처리할 때 메모리 사용량 최소화

### 1.2 HTML 크롤링 (YNA 뉴스)

**요구사항**: 하루치 수집, RSS 활용, 페이지 링크 자동 탐색 Bot

**구현 방식**:

```
RSS 피드 다운로드
    ↓ Rome RSS Parser로 기사 목록 추출
    ↓ 중복 제거 (articleId 기준)
    ↓ Semaphore 기반 병렬 크롤링 (동시 4개)
    ↓ 요청 간 지연 (250ms + jitter 150ms)
    ↓ MinIO 저장 + Kafka 이벤트 발행
```

**설정값**:

| 설정                        | 값    | 설명      |
|---------------------------|------|---------|
| `concurrency`             | 4    | 동시 요청 수 |
| `inter-request-delay-ms`  | 250  | 요청 간 지연 |
| `inter-request-jitter-ms` | 150  | 지연 랜덤화  |
| `connect-timeout-ms`      | 2000 | 연결 타임아웃 |
| `response-timeout-ms`     | 5000 | 응답 타임아웃 |

#### 1.3 API 연동 (YouTube)

**요구사항**: YouTube Developer API 활용, API 호출 장애 대응

**구현 방식**:

```
YouTube Data API v3 호출
    ↓ mostPopular 비디오 목록 조회
    ↓ 장애 시 Exponential Backoff 재시도
    ↓ 429/5xx 에러 자동 재시도
    ↓ 실패 시 DLQ 전송
```

**설정값**:

| 설정                | 값    | 설명        |
|-------------------|------|-----------|
| `max-attempts`    | 2    | 최대 재시도 횟수 |
| `base-backoff-ms` | 200  | 기본 백오프 시간 |
| `batch-size`      | 1000 | 배치 크기     |

---

## 2. 정제 시스템 (Refine System)

> **포트**: 9082 | **역할**: Kafka 메시지 수신 → 데이터 정제 → Kafka 메시지 발송

### 요구사항 충족 평가

| 시스템    | 요구사항                 | 상태       | 구현 방식                            |
|--------|----------------------|----------|----------------------------------|
| **정제** | 대량 데이터 수신 (초당 1000건) | ⚠️ 부분 충족     | 배치(50) × 동시성(10) × 2 = ~1000건/초  |
| **정제** | 카프카 컨슈머 튜닝           | ✅ 충족     | 11개 튜닝 포인트 적용                    |
| **정제** | 데이터 넘버링 (PK 부여)      | ✅ 충족     | `refined_<SHA256(source+id)>` 방식 |
| **정제** | 메타데이터 생성             | ✅ 충족     | 소스별 상세 메타데이터 생성                  |
| **정제** | FullText 추출          | ✅ 충족     | Wikitext/HTML/JSON 파싱            |

#### 2.1 대량 데이터 수신

**요구사항**: 초당 1000건 처리

**구현 방식**:

```
Kafka Consumer (concurrency=10, 파티션 수와 동일)
    ↓ 배치 수신 (max-poll-records=50)
    ↓ 병렬 처리 (ThreadPool: core=10, max=20)
    ↓ 수동 커밋 (ack-mode=manual)
```

**처리량 계산**:

```
이론적 처리량 = 배치크기(50) × 동시성(10) × 초당배치수
             = 50 × 10 × ~2 = ~1000건/초
```

#### 💡 왜 스레드 풀(core=10, max=20)을 이렇게 설정했는가?

- **Kafka concurrency와 매칭**: Consumer 스레드 수(10)와 동일하게 설정하여 병목 방지
- **CPU 바운드 작업 특성**: 정제 작업은 HTML/Wikitext 파싱으로 CPU를 많이 사용
- **max = core × 2 원칙**: 버스트 트래픽 시 2배까지 확장, I/O 대기 중 추가 처리 가능
- **queue-capacity=500**: 스레드가 바쁠 때 대기 큐, 너무 크면 메모리 낭비

### 2.2 카프카 컨슈머 튜닝

**적용된 튜닝 포인트**:

| 설정                      | 값              | 목적           |
|-------------------------|----------------|--------------|
| `max-poll-records`      | 50             | LAG 모니터링 최적화 |
| `fetch.min.bytes`       | 1MB            | 네트워크 효율화     |
| `fetch.max.wait.ms`     | 50ms           | 지연/처리량 균형    |
| `concurrency`           | 10             | 파티션 수와 매칭    |
| `ack-mode`              | MANUAL         | 정확한 처리 보장    |
| `enable-auto-commit`    | false          | 수동 커밋        |
| `session.timeout.ms`    | 45000          | 세션 유지        |
| `heartbeat.interval.ms` | 15000          | 하트비트 간격      |
| `isolation.level`       | read_committed | 커밋된 데이터만     |
| `max.poll.interval.ms`  | 600000         | 배치 처리 최대 시간  |

#### 💡 왜 max-poll-records를 50으로 설정했는가?

- **LAG 모니터링 정확도**: 값이 너무 크면(300+) Consumer LAG 지표가 실제보다 부풀려져 보임
- **처리 실패 시 재처리 범위 최소화**: 배치 중간에 실패하면 전체 배치를 재처리해야 하므로, 작은 배치가 복구에 유리
- **메모리 사용량 제어**: 50개 × 메시지 크기로 힙 메모리 사용량 예측 가능

#### 💡 왜 fetch.min.bytes를 1MB로 설정했는가?

- **네트워크 왕복 최소화**: 작은 메시지가 많을 때 1MB가 될 때까지 브로커가 대기 후 일괄 전송
- **브로커 부하 감소**: 잦은 fetch 요청으로 인한 브로커 CPU 사용량 절감
- **처리량 향상**: 한 번에 더 많은 데이터를 가져와 배치 처리 효율 증가

#### 💡 왜 concurrency를 파티션 수와 동일하게 맞추는가?

- **1:1 매칭 원칙**: Kafka에서 하나의 파티션은 Consumer Group 내 단 하나의 Consumer만 소비 가능
- **리소스 낭비 방지**: concurrency > 파티션 수이면 초과 스레드는 유휴 상태로 메모리만 낭비
- **병렬성 최대화**: concurrency < 파티션 수이면 일부 파티션이 하나의 스레드에 몰려 LAG 증가
- **현재 권장**: 파티션 10개 환경에서는 `concurrency: 10`이 최적

#### 💡 왜 ack-mode를 MANUAL로 설정했는가?

- **정확히 한 번(Exactly-Once) 처리 보장**: 메시지 처리가 완전히 끝난 후에만 커밋
- **데이터 유실 방지**: auto-commit은 처리 전에 커밋될 수 있어, 장애 시 메시지 유실 가능
- **재처리 가능**: 처리 중 예외 발생 시 커밋하지 않아 재시작 후 동일 메시지 재처리

#### 💡 왜 session.timeout.ms를 45초로 설정했는가?

- **리밸런싱 지연 방지**: 값이 너무 작으면 GC나 일시적 네트워크 지연에도 Consumer가 죽은 것으로 판단
- **장애 감지 속도 균형**: 값이 너무 크면 실제 장애 시 리밸런싱이 늦어져 처리 지연
- **heartbeat.interval.ms의 3배 원칙**: session.timeout(45s) = heartbeat(15s) × 3, 3번의 하트비트 실패 후 제외

#### 💡 왜 max.poll.interval.ms를 600초(10분)로 설정했는가?

- **대용량 배치 처리 허용**: Wikipedia 샤드(500페이지)나 대량 뉴스 처리 시 충분한 시간 확보
- **외부 서비스 지연 대비**: MongoDB, MinIO 등 외부 저장소 응답 지연 시에도 타임아웃 방지
- **리밸런싱 트리거 방지**: 이 시간 내에 poll()을 호출하지 않으면 Consumer가 그룹에서 제외됨

### 2.3 데이터 정제

**넘버링 (PK 부여)**:

```java
// 결정론적 ID 생성 - 멱등성 보장
String refinedId = "refined_" + HashUtils.sha256Hex(source + pageId);
// 예: refined_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
```

**FullText 추출**:

| 소스        | 파서               | 처리                                  |
|-----------|------------------|-------------------------------------|
| Wikipedia | Bliki (Wikitext) | Wikitext → HTML → PlainText         |
| YNA News  | JSoup (HTML)     | HTML → 본문 선택 → 광고/UI 제거 → PlainText |
| YouTube   | Jackson (JSON)   | JSON → 구조화된 필드 추출                   |

---

## 3. 색인 시스템 (Index System)

> **포트**: 9083 | **역할**: Kafka 메시지 수신 → Elasticsearch로 데이터 색인

### 요구사항 충족 평가

| 시스템    | 요구사항                 | 상태       | 구현 방식                       |
|--------|----------------------|----------|-----------------------------|
| **색인** | 대량 데이터 수신 (초당 1000건) | ⚠️ 부분 충족     | 배치(100) × 동시성(10) = 1000건/초. 그러나 현재 600건/초 |
| **색인** | 카프카 컨슈머 튜닝           | ✅ 충족     | 11개 튜닝 포인트 적용               |
| **색인** | 형태소 분석 색인            | ✅ 충족     | Elasticsearch Nori 분석기      |
| **색인** | 자신만의 형태소 분석기         | ⚠️ 부분 충족 | Nori + 자체 불용어 필터링           |

### 3.1 대량 데이터 수신

**처리량**:

```
처리량 = 배치크기(100) × 동시성(10) × 초당배치수 ≈ 1000~2000건/초
```

**설정**:

| 설정                 | 값   | 설명         |
|--------------------|-----|------------|
| `max-poll-records` | 100 | 배치 크기      |
| `concurrency`      | 10  | 파티션 수와 매칭  |
| `BULK_SIZE`        | 500 | ES Bulk 크기 |

#### 💡 왜 max-poll-records를 100으로 설정했는가?

- **Elasticsearch Bulk API 최적화**: ES는 개별 문서 색인보다 Bulk 색인이 10배 이상 빠름
- **정제 시스템보다 큰 배치**: 색인 작업은 정제(파싱/변환)보다 I/O 바운드이므로 큰 배치가 유리
- **BULK_SIZE(500)와의 관계**: 100개씩 여러 번 poll하여 500개가 모이면 Bulk 요청 전송

#### 💡 왜 BULK_SIZE를 500으로 설정했는가?

- **ES 최적 배치 크기**: Elasticsearch 공식 권장은 5-15MB 또는 1000-5000 문서
- **메모리 사용량 균형**: 500 × 평균 문서 크기(~10KB) = ~5MB로 적정 수준
- **실패 시 재시도 범위**: Bulk 실패 시 500개 단위로 재시도, 너무 크면 재시도 부담 증가
- **네트워크 효율**: 너무 작으면 HTTP 오버헤드 증가, 너무 크면 타임아웃 위험

#### 💡 왜 스레드 풀(core=10, max=20)을 이렇게 설정했는가?

- **I/O 바운드 작업 특성**: ES 색인은 네트워크 I/O 대기가 많아 CPU 코어 수보다 많은 스레드 필요
- **core-pool-size=10**: 평상시 유지할 스레드 수, ES 연결 풀 크기와 유사하게 설정
- **max-pool-size=20**: 버스트 트래픽 시 임시 확장, 너무 크면 ES 연결 고갈 위험
- **queue-capacity=500**: 스레드가 모두 바쁠 때 대기 큐, BULK_SIZE와 동일하게 설정

### 3.2 형태소 분석 색인

#### Elasticsearch 분석기 설정([elasticsearch-index-settings.json](index-system/src/main/resources/elasticsearch-index-settings.json)):

```json
{
  "analyzer": {
    "korean_analyzer": {
      "type": "custom",
      "tokenizer": "nori_tokenizer",
      "filter": [
        "lowercase",
        "nori_part_of_speech",
        "korean_stopwords",
        "nori_readingform"
      ]
    }
  }
}
```

#### 품사 필터링 (명사 위주 추출)

```json
"nori_part_of_speech": {
"stoptags": [
"E", "IC", "J", "MAG", "MAJ", "MM", "SP", "SSC", "SSO",
"SC", "SE", "XPN", "XSA", "XSN", "XSV", "UNA", "NA", "VSV"
]
}
```

- 제거: 종료부호(E), 감탄사(IC), 조사(J), 수정자(MM) 등
- 유지: 명사(N*), 주요 동사/형용사

### 3.3 자체 불용어 필터링

#### Java 레벨 추가 필터링([ElasticsearchKeywordExtractor.java](index-system/src/main/java/com/realtime/index/application/service/ElasticsearchKeywordExtractor.java)):

```java
private static final Set<String> STOPWORDS = Set.of(
        // 한국어 조사/어미
        "있다", "있는", "하다", "되다", "이다", "것", "수", "등", "그", "및",
        "이", "가", "을", "를", "의", "에", "와", "과", "도", "만",
        // Wikipedia 메타데이터
        "탄생", "사망", "기년", "사건", "분류", "연호", "왕조", "출생"
);

// 키워드 추출 로직
Map<String, Long> tokenFrequency = response.tokens().stream()
        .filter(token -> token.length() >= 2)           // 최소 길이
        .filter(token -> !isStopword(token))            // 불용어 제거
        .collect(groupingBy(identity(), counting()));   // 빈도 계산
```

### 3.4 Elasticsearch 설정

| 설정      | 값      | 설명     |
|---------|--------|--------|
| 샤드 수    | 3      | 분산 처리  |
| 레플리카 수  | 1      | 고가용성   |
| 리프레시 간격 | 5초     | 실시간 검색 |
| Bulk 크기 | 500 문서 | 배치 색인  |
| JVM 메모리 | 1GB    | 힙 메모리  |

---

## 4. 서빙 시스템 (Serving System)

> **포트**: 9086 | **역할**: Elasticsearch 쿼리 및 Redis 캐싱

### 요구사항 충족 평가

| 시스템    | 요구사항            | 상태       | 구현 방식                      |
|--------|-----------------|----------|----------------------------|
| **서빙** | 명사 추출           | ⚠️ 부분 충족 | Nori POS 태그 필터링            |
| **서빙** | 빈도수 계산 + 가중치    | ✅ 충족     | 소스/시간 가중치 적용               |
| **서빙** | 데이터 저장 (ES)     | ✅ 충족     | Elasticsearch에 키워드 저장      |
| **서빙** | 데이터 캐시 (상위 10건) | ✅ 충족     | Redis 캐싱 (하루 1회 갱신, 24시간 TTL) |

### 4.1 오늘의 키워드 생성

#### 처리 흐름

```
KeywordRankingScheduler (매일 자정 + 시작 시 10초 후)
    ↓ Elasticsearch 키워드 집계 (최근 24시간)
    ↓ 소스별 가중치 적용
    ↓ 시간별 가중치 적용 (하루 기준)
    ↓ 점수 계산 및 랭킹
    ↓ Redis 캐시 저장 (TTL: 24시간)
```

#### 키워드 점수 계산 가중치 설정

- 소스별 가중치

| 소스             | 가중치   | 설명              |
|----------------|-------|-----------------|
| NEWS_YNA       | 100.0 | 실시간 뉴스 (최고 가중치) |
| SNS_YOUTUBE    | 5.0   | SNS 트렌드         |
| DOCS_WIKIPEDIA | 0.1   | 배경 지식           |

- 시간별 가중치 (하루 기준)

| 시간 범위 | 가중치  | 설명     |
|-------|------|--------|
| 당일    | 10.0 | 오늘 트렌드 |
| 전일    | 5.0  | 어제 트렌드 |
| 그 이전  | 1.0  | 기본값    |

#### 점수 계산 공식

```
점수 = docCount × sourceWeight × timeWeight
```

**예시**: "한동훈" 키워드

```
YNA에서 15건 (2시간 전): 15 × 100.0 × 5.0 = 7,500.0
YouTube에서 3건 (30분 전): 3 × 5.0 × 10.0 = 150.0
────────────────────────────────────────────────────
최종 점수: 7,650.0
```

### 4.2 데이터 캐시

#### Redis 캐싱([KeywordRankingCache.java](serving-system/src/main/java/com/realtime/serving/infrastructure/cache/KeywordRankingCache.java))

```java
private static final String RANKING_KEY = "trending:keywords:full";
private static final long CACHE_TTL_HOURS = 24;

// 캐시 저장
public void saveRanking(List<RankedKeyword> rankedKeywords) {
    String json = objectMapper.writeValueAsString(rankedKeywords);
    redisTemplate.opsForValue().set(RANKING_KEY, json,
            CACHE_TTL_HOURS, TimeUnit.HOURS);
}

// 상위 N개 조회
public List<RankedKeyword> getTopKeywords(int limit) {
    return allKeywords.stream().limit(limit).toList();
}
```

#### 갱신 스케줄러([KeywordRankingScheduler.java](serving-system/src/main/java/com/realtime/serving/infrastructure/scheduler/KeywordRankingScheduler.java))

```java

@Scheduled(cron = "0 0 0 * * *", zone = "Asia/Seoul")  // 매일 자정
public void updateKeywordRanking() {
    // 1. 최근 24시간 키워드 집계
    Map<ContentSource, List<RawKeyword>> keywordsBySource =
            aggregationService.aggregateAllKeywords(
                    LocalDateTime.now().minusHours(24));

    // 2. 스코어링 및 랭킹
    List<RankedKeyword> ranked =
            scoringService.calculateRanking(keywordsBySource);

    // 3. Redis 저장
    rankingCache.saveRanking(ranked);
}
```

---

## 기술 스택

### Core

| 기술          | 버전    | 용도    |
|-------------|-------|-------|
| Java        | 21    | 런타임   |
| Spring Boot | 3.3.1 | 프레임워크 |
| Gradle      | 8.x   | 빌드 도구 |

### 데이터 저장

| 기술            | 버전     | 용도        |
|---------------|--------|-----------|
| MySQL         | 8.0    | 메타데이터 저장  |
| Elasticsearch | 8.11.0 | 검색 인덱스    |
| MongoDB       | 7.0    | 정제 데이터 저장 |
| Redis         | 7.2    | 캐싱        |
| MinIO         | latest | 원본 객체 저장  |

### 메시징

| 기술           | 버전    | 용도       |
|--------------|-------|----------|
| Apache Kafka | 7.4.0 | 이벤트 스트리밍 |

### 크롤링 & 파싱

| 기술          | 버전     | 용도          |
|-------------|--------|-------------|
| Selenium    | 4.16.1 | 동적 크롤링      |
| JSoup       | 1.18.1 | HTML 파싱     |
| Apache Tika | 2.9.2  | 문서 파싱       |
| Bliki       | 3.1.0  | Wikitext 파싱 |
| Rome        | 1.18.0 | RSS 파싱      |

### 압축

| 기술               | 용도           |
|------------------|--------------|
| Zstandard (zstd) | Kafka 메시지 압축 |
| Snappy           | 빠른 압축        |
| Commons Compress | bz2 압축 해제    |

### 모니터링

| 기술         | 용도               |
|------------|------------------|
| Prometheus | 메트릭 수집           |
| Grafana    | 대시보드             |
| Kibana     | Elasticsearch UI |

---

## 인프라 구성

### 서비스 포트 맵핑

| 서비스              | 포트    | 용도          |
|------------------|-------|-------------|
| **애플리케이션**       |       |             |
| Collector System | 9080  | 데이터 수집 API  |
| Refine System    | 9082  | 데이터 정제 API  |
| Index System     | 9083  | 검색 색인 API   |
| Serving System   | 9086  | 실시간 키워드 API |
| **데이터베이스**       |       |             |
| MySQL            | 3306  | 메타데이터       |
| Elasticsearch    | 9200  | 검색 엔진       |
| MongoDB          | 27017 | 문서 저장소      |
| Redis            | 6379  | 캐싱          |
| MinIO API        | 9000  | 객체 스토리지     |
| MinIO Console    | 19000 | MinIO 관리 UI |
| **메시징**          |       |             |
| Kafka            | 9092  | 메시지 브로커     |
| **모니터링**         |       |             |
| Grafana          | 18083 | 대시보드        |
| Prometheus       | 19090 | 메트릭         |
| Kibana           | 15601 | ES 모니터링     |
| **개발 도구**        |       |             |
| Kafka UI         | 18080 | Kafka 관리    |
| Mongo Express    | 18081 | MongoDB UI  |
| Redis Commander  | 18082 | Redis UI    |

### Docker Compose 프로파일

| 프로파일        | 서비스                                      | 용도     |
|-------------|------------------------------------------|--------|
| `collector` | MySQL, Kafka, MinIO                      | 수집 시스템 |
| `refine`    | MySQL, Kafka, MinIO, MongoDB, Redis      | 정제 시스템 |
| `index`     | MySQL, Kafka, Elasticsearch, MongoDB     | 색인 시스템 |
| `serving`   | MySQL, Elasticsearch, Redis              | 서빙 시스템 |
| `dev`       | Kafka UI, Mongo Express, Redis Commander | 개발 도구  |

### Elasticsearch 커스텀 이미지

```dockerfile
FROM docker.elastic.co/elasticsearch/elasticsearch:8.11.0
RUN bin/elasticsearch-plugin install --batch analysis-nori
```

한국어 형태소 분석을 위해 Nori 플러그인이 포함된 커스텀 이미지 사용

---

## 설치 및 실행

### 사전 요구사항

- Java 21+
- Docker & Docker Compose
- Gradle 8.x

### 1. 프로젝트 클론 및 환경 설정

```bash
git clone <repository-url>
cd realtime-ju

# 환경 변수 설정
cp .env.example .env
# .env 파일을 열어 필요한 값 수정
```

### 2. 인프라 실행

```bash
# 전체 인프라 + 모니터링 시작
./scripts/compose-up-all.sh

# 또는 개별 시스템별 실행
./scripts/compose-up-collector.sh   # 수집 시스템용
./scripts/compose-up-refine.sh      # 정제 시스템용
./scripts/compose-up-index.sh       # 색인 시스템용
./scripts/compose-up-serving.sh     # 서빙 시스템용

# 전체 중지 및 볼륨 정리
./scripts/compose-down.sh
```

### 3. 애플리케이션 빌드

```bash
# 전체 프로젝트 빌드
./gradlew clean build

# 공통 라이브러리 발행
./gradlew :common-lib:publishToMavenLocal
```

### 4. 시스템 실행

```bash
# 각 시스템 개별 실행
./gradlew :collector-system:bootRun
./gradlew :refine-system:bootRun
./gradlew :index-system:bootRun
./gradlew :serving-system:bootRun
```

### 5. 테스트

```bash
# 전체 테스트
./gradlew test

# 특정 모듈 테스트
./gradlew :collector-system:test
./gradlew :refine-system:test
./gradlew :index-system:test
./gradlew :serving-system:test
```

