# Serving System | 실시간 검색어 API 서버

- 실시간 트렌딩 키워드를 제공하는 REST API 서버
- Elasticsearch 기반 검색과 Redis 캐싱을 통해 빠른 응답을 제공
- 하루 기준으로 가장 높은 Score 키워드 10건 캐싱

## ☁️ API 명세

### 트렌딩 키워드 API

#### GET /api/v1/keywords/trending

실시간 트렌딩 키워드 목록 조회

**Request**

```
GET /api/v1/keywords/trending?limit=10
```

| 파라미터  | 타입  | 필수 | 기본값 | 설명                |
|-------|-----|----|-----|-------------------|
| limit | int | N  | 10  | 조회할 키워드 수 (1-100) |

**Response (200 OK)**

```json
{
  "data": [
    {
      "keyword": "한동훈",
      "score": 7650.5,
      "rank": 1,
      "sources": [
        {
          "source": "yna",
          "url": "https://www.yna.co.kr/view/AKR20250902...",
          "title": "한동훈 대표의 대선 도전"
        },
        {
          "source": "youtube",
          "url": "https://www.youtube.com/watch?v=...",
          "title": "한동훈 인터뷰"
        }
      ]
    }
  ],
  "metadata": {
    "lastUpdatedAt": 1725270600000,
    "total": 250
  }
}
```

## ☁️ 프로젝트 구조

```
src/
├── main/
│   ├── java/com/juju/realtime/
│   │   ├── application/          # 애플리케이션 서비스
│   │   ├── domain/              # 도메인 모델
│   │   ├── infrastructure/      # 인프라스트럭처
│   │   ├── presentation/        # 프레젠테이션 계층
│   │   └── global/              # 글로벌 설정
│   └── resources/
│       └── application.yml      # 애플리케이션 설정
└── test/                        # 테스트 코드
```

### 🔄 의존성 방향

```
Presentation → Application → Domain ← Infrastructure
```

- **Presentation**: API 컨트롤러 (외부 인터페이스)
- **Application**: 유스케이스 조정, 여러 도메인 서비스 조합
- **Domain**: 핵심 도메인 모델과 비즈니스 로직
- **Infrastructure**: 외부 시스템 연동 (Repository 구현체)

