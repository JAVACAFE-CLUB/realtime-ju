# Serving System | 실시간 검색어 API 서버

- 실시간 트렌딩 키워드를 제공하는 REST API 서버
- Elasticsearch 기반 검색과 Redis 캐싱을 통해 빠른 응답을 제공

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
