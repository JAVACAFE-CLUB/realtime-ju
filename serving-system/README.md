# Realtime Keyword

## ☁️ 기술 스택

- **Java 21**
- **Spring Boot 3.3.1**
- **Spring Data JPA**
- **MySQL 8.0**
- **Gradle**
- **Docker & Docker Compose**

## ️☁️ 요구사항

- Java 21 이상
- Docker & Docker Compose
- Gradle (또는 Gradle Wrapper 사용)

## ☁️ 설치 및 실행

### 1. 프로젝트 클론 및 환경 설정

```bash
# 프로젝트 클론
git clone <repository-url>
cd realtime

# 환경 변수 설정
cp .env.example .env

# 필요에 따라 .env 파일의 환경 변수를 수정하세요
```

### 2. 애플리케이션 실행

```bash
# MySQL 컨테이너 실행
docker-compose up -d mysql

# 애플리케이션 실행
./gradlew bootRun
```

> **애플리케이션 접속**: http://localhost:8080

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

## ️☁️ 테스트 실행

```bash
# 전체 테스트 실행
./gradlew test

# 특정 테스트 클래스 실행
./gradlew test --tests KeywordControllerTest
```

<br />

<img src="https://raw.githubusercontent.com/Tarikul-Islam-Anik/Animated-Fluent-Emojis/master/Emojis/Smilies/Ghost.png" alt="Ghost" width="50" height="50" />
<img src="https://raw.githubusercontent.com/Tarikul-Islam-Anik/Animated-Fluent-Emojis/master/Emojis/Smilies/Ghost.png" alt="Ghost" width="50" height="50" />
<img src="https://raw.githubusercontent.com/Tarikul-Islam-Anik/Animated-Fluent-Emojis/master/Emojis/Smilies/Ghost.png" alt="Ghost" width="50" height="50" />
