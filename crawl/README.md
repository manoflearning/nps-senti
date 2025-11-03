## nps-senti: Unified Crawler

목표: 기존 scrape 파이프라인과 분리된 단일 수집 라인(crawl/)을 구축한다. 합법·무료 범위에서 회수(Recall)와 신선도를 균형 있게 확보한다.

---

## 구현 현황 (2025-11-03)

- `crawl/core/` 하위에 Discover → Fetch → Extract → Store 단계로 구성.
- 소스: GDELT Doc API, 주요 커뮤니티(디시/보배/에펨/MLBPARK/더쿠/뽐뿌), YouTube(설명+상위 댓글)만 사용.
- `langdetect + trafilatura`로 품질/언어 처리 후 JSONL로 저장. (수집 단계에서는 유사 중복 제거 없음)
- `quality` 파라미터로 키워드·스코어 기준 제어.
- 실행 결과는 `data_crawl/crawl.jsonl`(또는 설정한 파일명)로 누적. 인덱스는 동일 폴더 `_index.json`.

### 실행 방법
```bash
# 의존성 설치(최초 1회)
uv --project crawl sync

# Unified 파이프라인 실행 (예: 5건만 시험 수집)
uv --project crawl run python -m crawl.cli --max-fetch 5 --log-level INFO

# GDELT가 느리거나 불필요하면 비활성화
uv --project crawl run python -m crawl.cli --no-gdelt --max-fetch 5 --log-level INFO
```

옵션
- `--params`: 기본 `crawl/config/params.yaml` 대신 다른 설정 파일 사용
- `--max-fetch`: 실행 시 한 번에 가져올 최대 문서 수(기본 params.yaml의 limits.max_fetch_per_run)
-- `YOUTUBE_API_KEY` 환경 변수가 설정되어 있으면 YouTube 설명/상위 댓글을 포함한다.
- `--no-gdelt`: 실행 중 GDELT 탐색 비활성화(느림/쿼터 이슈 회피용)

최근 테스트(2025-11-03)
– 최근 테스트: GDELT 수백 건 후보, 커뮤니티 보드별 수십~수백 건 발견. 품질/중복 필터 후 수십 건 저장.

원칙
- 기존 코드/데이터(`data_raw/`, `scrape/`) 변경 금지. 신규 산출물은 리포지토리 최상위 `data_crawl/` 하위에 저장.
- 로봇 배려(robots.txt) 및 각 소스 정책 준수. 우회·회피 전략은 배제.
- 비용 0원 지향: 공개 인덱스/아카이브/무료 API만 사용. 대용량은 단계적·선별 수집.
- 최신성/커버리지 균형: GDELT로 빠른 후보, 커뮤니티 목록으로 최신 글 보강.

키워드(초안)
- KO: 국민연금, 국민연금공단, 연금개혁, 연금개편, 연금, 퇴직연금, 개인연금, 연기금, 기금운용, 노후준비, 노후연금
- EN: National Pension Service, NPS Korea, Korea National Pension, pension reform, pension fund, retirement pension, fund management
- 약어/변형: NPS, 국민 연금, 국 민 연 금, 연금 개혁, 연금 개편

대상 범주
- 뉴스/블로그(주로 GDELT), 커뮤니티 게시판(목록), YouTube(설명/댓글)

출력 형태(요약)
- 1차 산출: JSONL(문서 단위)만 사용. 상세 스키마는 아래 “Unified Schema” 참조.
- 중간 산출: URL 후보 목록, 스냅샷 링크(IA/CC), 로그/오류 리포트.

---

## 파이프라인(요약)
1) Discover: GDELT(키워드/언어/기간), 커뮤니티 목록(보드별), YouTube 검색
2) Fetch: 라이브 fetch만 사용(robots 준수). 스냅샷/이미지/영상 불수집.
3) Extract: trafilatura로 본문 텍스트, YouTube는 설명+상위 댓글 병합
4) Score: 품질 기준(언어/키워드)
5) Store: `data_crawl/crawl.jsonl`에 저장, `_index.json`으로 중복 차단

산출 기대치(키워드 규모에 따라)
- 뉴스/블로그/포털: 수만~수십만 문서.
- YouTube: 수천~수만 메타/자막(쿼터 한도 내).

운영 메모
- 대역폭·스토리지 절약: 원문 HTML 미보관, 텍스트+스냅샷 URL만 저장 우선.
- 배치 우선: 주기적 증분(주 1회) + 대용량 과거 백필 채널 분리.

---

<삭제> Plan B는 더 이상 사용하지 않습니다.

운영 메모
- 헤드리스(Playwright 등)는 기본 배제. 꼭 필요한 소수 도메인만 예외 처리(별도 승인 후).
- 피드 기반은 안정적·유지보수 낮음. 도메인별 상태 모니터링 필요.

---

## Unified Schema (JSONL)

레코드 단위: 문서(document) 또는 동영상(video). 공통 필드와 소스별 보조 필드를 구분.

공통
```json
{
  "id": "sha1(url_norm + text_hash)",
  "source": "gdelt|youtube|dcinside|bobaedream|fmkorea|mlbpark|theqoo|ppomppu",
  "url": "https://...",
  "snapshot_url": null,
  "title": "...",
  "text": "...",                         
  "lang": "ko",
  "published_at": "YYYY-MM-DDTHH:MM:SSZ|null",
  "authors": ["..."],
  "discovered_via": {"type": "gdelt", "meta": {"...": "..."}},
  "quality": {"score": 0.0, "reasons": ["lang", "coverage", "keyword_hits"]},
  "dup": {},
  "crawl": {"run_id": "2025-11-03-1200", "fetched_at": "..."}
}
```

YouTube 보조
```json
{
  "video_id": "...",
  "channel_id": "...",
  "channel_title": "...",
  "captions": [{"lang": "ko", "text": "..."}],
  "stats": {"views": 0, "likes": 0, "comments": 0}
}
```

메타 규칙
- `published_at`은 소스 메타 또는 본문 추정(날짜 패턴 추론)으로 채움. 불명확 시 null.
- 텍스트 키워드 밀도 등 기준 미달은 드롭 또는 `quality.score` 낮게 설정.
- 스냅샷/이미지/영상은 저장하지 않음(텍스트만).

---

## 아키텍처(로직 레벨)

컴포넌트
1) Discoverer
   - 입력: 키워드, 날짜 범위, 언어. 출력: URL/스냅샷/비디오ID 후보.
2) Fetcher
   - 정책: robots 준수, 도메인별 동시성/초당 요청 제한, 재시도・백오프. 라이브 페치만 사용.
3) Extractor
   - 도구: trafilatura/readability, 인코딩 정규화, 언어 감지(fasttext/cld3), 날짜 추정.
4) Deduper/Scorer
   - URL 정규화(쿼리 파라미터 화이트리스트), canonical 태그 반영, SimHash/MinHash.
5) Storage
   - JSONL 저장, run_id 단위 롤업, 인덱스(개념): url_norm, published_at, lang

운영
- 실행 방식: 일회성 대량 백필(run_id로 구분) + 증분 배치. 컨트롤 파일로 대상/기간 분할.
- 장애/로그: 실패 URL 큐 분리, 영구 로그(JSONL) 보관.

---

## 파라미터(설정)

기본은 설정 파일 또는 CLI 인자로 주입(구현 시 확정). 날짜는 ISO8601(UTC) 권장.

- time_window.start_date: 기본값 "2025-01-01T00:00:00Z"
  - 최초 백필 시작일. 필요 시 "2010-01-01T00:00:00Z" 등으로 자유 변경 가능.
- time_window.end_date: 기본값 null(현재까지). 지정 시 해당 시점까지만 수집.
- lang: 기본 "ko" (필요 시 ["ko","en"] 등 확장 가능).
- output.root: 기본 `data_crawl/` (변경 가능).
- run_id: 자동 생성(예: YYYYMMDD-hhmm) 또는 수동 지정.
 - sources.gdelt.enabled: true/false 로 GDELT 탐색 on/off 제어
  - sources.gdelt.max_concurrency: 동시 요청 수(기본 4)
  - sources.gdelt.max_days_back: 종료시점 기준 최대 조회 일수(범위 축소)
  - sources.gdelt.pause_between_requests: 요청 간 대기(초)

---

## 디렉토리 구조(초안)

```
crawl/
  README.md                  # 본 문서
  config/
    params.yaml              # 실행 파라미터(키워드/도메인/기간 등 통합)
  cli.py                     # CLI 진입점
  core/
    config.py                # 설정 로더
    models.py
    utils.py
    pipeline.py
    discovery/               # GDELT/YouTube/Forums discoverers
      forums.py
    fetch/                   # 라이브 fetcher(robots 준수)
    extract/                 # 본문·메타 추출(YouTube 댓글 보강)
    dedupe/                  # (삭제됨) 중복 처리 모듈은 사용하지 않음
    storage/                 # JSONL writer/index
data_crawl/
  crawl.jsonl                # 통합 산출
```

초기에는 문서와 설정만 두고, 구현 시 스크립트는 별도 하위 폴더(`cmd/`, `lib/`)에 추가. 산출 폴더(`data_crawl/…`)는 실행 시 자동 생성.

---

## 마일스톤 & 수락 기준

M1. 설계 고도화(이 문서) — 완료 기준
- Plan A/B 범위와 스키마 합의, 키워드 목록 초안 확정.

M2. Plan A(오픈데이터) PoC — 3~5일
- CC/GDELT/IA에서 최소 1만+ URL 후보 발견.
- 추출 파이프라인 적용 후 유효 문서(ko, 길이>=500자) 3천+ 확보.
- 중복 제거 동작(근접중복 그룹화)과 품질 스코어링 리포트.

M3. Plan B(피드/범용) PoC — 3~5일
- RSS/사이트맵 200+ 소스 수집, 유효 문서 2천+ 확보.
- 도메인별 rate-limit/robots 준수 로깅.

M4. 통합/정리 — 2~3일
- Unified Schema로 통합 JSONL 산출(단일 포맷), 중복 통합.
- 샘플 검수 리포트(품질/도메인 분포/기간 분포).

수락 기준(예)
- 재현성: 동일 run_id 재실행 시 동일 결과(±스냅샷 변동) 보장.
- 정합성: 필수 필드 채움률(>95%), 언어 정확도(ko F1>0.95) 달성.

---

## 리스크 & 완화

- 대역폭/용량: 대량 페치 시 속도↓ → 스냅샷 우선, 텍스트만 저장, 배치 분할.
- 품질 편차: CC/GDELT 노이즈 → 길이/밀도/언어/키워드 스코어로 필터.
- 정책/차단: robots 준수, 요청 속도 제한. 우회 전략은 사용하지 않음.
- 무료 API 쿼터: YouTube 등은 저빈도 증분 + 자막 위주.

---

## 다음 액션(합의 필요)

1) 키워드 리스트 확정(추가/제거 제안 환영)
2) Plan A 우선순위: CC vs GDELT 시작점 선택
3) 산출 포맷: JSONL만 사용(확정)
4) 최초 백필 시작 기본값: 2025-01-01 (자유롭게 변경 가능)

주석: 구현 시작 전까지는 본 문서만 유지하며, 기존 `data_raw/` 및 `scrape/`에는 영향 없음.
