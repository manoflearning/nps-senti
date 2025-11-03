## nps-senti: Crawling Plan (A+B)

목표: 기존 scrape 파이프라인과 분리된 신규 수집 라인(crawl/)을 설계한다. “대상 범위: 모두”, “히스토리 최대”, “예산 0원”을 전제로, 합법·무료 범위에서 회수(Recall)를 극대화한다.

---

## Plan A 구현 현황 (2025-11-03)

- `crawl/plan_a/` 하위에 Discover → Fetch → Extract → Deduplicate → Store 단계가 파이프라인으로 구현됨.
- Common Crawl(CDX API) + GDELT Doc API를 통해 URL을 대량 확보하고, Internet Archive/원문을 이용해 본문을 추출한다.
- `langdetect + trafilatura + simhash` 기반 품질/언어 체크 및 중복 제거를 수행하여 Unified Schema(JSONL)로 저장한다.
- `quality` 파라미터로 최소 본문 길이·키워드 히트·점수 기준을 제어하며, 조건 미달 문서는 저장하지 않는다.
- `data_crawl/plan_a/_index.json`에 누적 문서 ID를 적재해 run 간 중복을 차단하고, CLI 통계에 품질/중복 세부 지표를 노출한다.
- 실행 결과는 `data_crawl/plan_a/plan_a.jsonl`(단일 JSONL DB)로 누적된다. run_id는 각 레코드의 `crawl.run_id` 필드로 식별한다.
- `sources.gdelt` 설정으로 30일 단위의 기간을 중첩(2일) 분할하며, 각 블록별 `max_records`를 제어해 API rate-limit에 대응한다.
- `sources.commoncrawl` 설정은 최신 9개 인덱스를 순회하며 도메인별 80건까지 캡처한다(서버 미가용 시 자동 스킵 후 재시도).
- `sources.forums` 설정을 통해 디시인사이드/보배드림/에펨코리아/MLBPARK/더쿠/뽐뿌 등 비공식 게시판의 최신 글 URL을 적극적으로 수집한다(로봇 준수, 게시글 본문은 Fetch 단계에서 처리).

### 실행 방법
```bash
# 의존성 설치(최초 1회)
uv --project crawl sync

# Plan A 파이프라인 실행 (예: 5건만 시험 수집)
uv --project crawl run python -m crawl.plan_a.cli --max-fetch 5 --log-level INFO
```

옵션
- `--params`: 기본 `crawl/config/params.yaml` 대신 다른 설정 파일 사용
- `--max-fetch`: 실행 시 한 번에 가져올 최대 문서 수(기본 params.yaml의 limits.max_fetch_per_run)
- `YOUTUBE_API_KEY` 환경 변수가 설정되어 있으면 YouTube 메타/자막까지 포함된다.

최근 테스트(2025-11-03)
- Discover: Common Crawl 0건(일시적 미가용), GDELT 457건
- Fetch: 30건 요청 → 저장 13건 / 품질 탈락 0건 / 중복 차단 17건
- 산출 예시: `data_crawl/plan_a/plan_a.jsonl` (2025-01-01~현재, 12개 도메인, 평균 본문 길이 ≈5K자)
  - `data_crawl/plan_a/runs/`에는 실행별 원본 JSONL을 보관(옵션).
  - `_index.json`은 전체 문서 ID 스냅샷을 유지하고, plan_a.jsonl에만 append 한다.

원칙
- 기존 코드/데이터(`data_raw/`, `scrape/`) 변경 금지. 신규 산출물은 리포지토리 최상위 `data_crawl/` 하위에 저장.
- 로봇 배려(robots.txt) 및 각 소스 정책 준수. 우회·회피 전략은 배제.
- 비용 0원 지향: 공개 인덱스/아카이브/무료 API만 사용. 대용량은 단계적·선별 수집.
- 히스토리 우선: 최신성보다 과거 커버리지 확보를 최우선 목표로.

키워드(초안)
- KO: 국민연금, 국민연금공단, 연금개혁, 연금개편, 연금, 퇴직연금, 개인연금, 연기금, 기금운용, 노후준비, 노후연금
- EN: National Pension Service, NPS Korea, Korea National Pension, pension reform, pension fund, retirement pension, fund management
- 약어/변형: NPS, 국민 연금, 국 민 연 금, 연금 개혁, 연금 개편

대상 범주
- 뉴스/포털, 블로그/개인 사이트, 동영상(YouTube 중심), 공개 아카이브(Internet Archive, Common Crawl), 피드(RSS/Atom), 사이트맵, 일부 포럼(공개 아카이브 경유)

출력 형태(요약)
- 1차 산출: JSONL(문서 단위)만 사용. 상세 스키마는 아래 “Unified Schema” 참조.
- 중간 산출: URL 후보 목록, 스냅샷 링크(IA/CC), 로그/오류 리포트.

---

## Plan A — 오픈데이터 중심(히스토리 최대)

핵심 소스
1) Common Crawl (CC-MAIN, News)
   - 사용: CDX API로 키워드 검색(페이지 타이틀/URL), WARC/WET 텍스트 추출.
   - 장점: 무료·대용량·장기 히스토리. 단점: 노이즈/중복 많음, 처리량 관리 필요.
2) GDELT v2 (Events, Mentions, GKG, 2.1 Doc Lists)
   - 사용: 한국어 기사/블로그 URL·메타 대량 확보(쿼리=키워드+언어=ko).
   - 장점: 뉴스·온라인 미디어 커버 광범위. 단점: 본문은 직접 페치/아카이브 참조 필요.
3) Internet Archive (Wayback/CDX)
   - 사용: 발견 URL의 과거 스냅샷 검색→본문 텍스트 추출.
4) YouTube(보강)
   - 사용: Data API 무료 쿼터로 키워드/채널·기간 검색→메타/자막 역사 수집(가능 범위).

파이프라인(Plan A)
1) Discover
   - CC/GDELT/IA/YouTube에서 URL/스냅샷/비디오ID 후보 대량 수집.
   - 필터: 언어(ko 우선), 키워드 매칭, 중복 URL 제거(정규화).
2) Fetch
   - IA/CC 스냅샷 우선(원 서버 과부하 방지, 정책 준수). 스냅샷 없음 시 원문 요청.
   - 도메인별 rate limit, 재시도·백오프.
3) Extract
   - 정적 HTML: trafilatura/readability로 본문·메타 추출. YouTube: 메타/자막.
   - 언어 감지(ko), 길이·밀도 기준으로 저품질 필터.
4) Deduplicate & Score
   - URL 정규화+문서 SimHash/MinHash로 근접중복 제거.
   - 품질 스코어(도메인 신뢰, 길이, 언어 확률, 키워드 커버리지).
5) Store
   - `data_crawl/plan_a/`에 JSONL 저장(+스냅샷 링크, 소스 트레이스).

산출 기대치(키워드 규모에 따라)
- 뉴스/블로그/포털: 수만~수십만 문서.
- YouTube: 수천~수만 메타/자막(쿼터 한도 내).

운영 메모
- 대역폭·스토리지 절약: 원문 HTML 미보관, 텍스트+스냅샷 URL만 저장 우선.
- 배치 우선: 주기적 증분(주 1회) + 대용량 과거 백필 채널 분리.

---

## Plan B — 피드 + 범용 크롤러(커버리지 보강)

핵심 소스
1) RSS/Atom, 사이트맵(sitemap.xml)
   - 언론사·블로그·기관·포털 서브섹션. 과거 피드도 최대 수집.
2) 공식 API(무료)
   - YouTube Data API, 일부 포털/블로그 플랫폼 공개 API.
3) 제한적 범용 크롤링
   - 정적 페이지 위주 링크 추적(BFS+우선순위), 동적 렌더링은 예외적으로만.

파이프라인(Plan B)
1) Discover
   - RSS/사이트맵 리스트 시드 → 신규/과거 항목 파싱.
   - 검색 API(무료 쿼터)로 키워드 기반 URL 후보 추가.
2) Fetch
   - 원 서버 호출이 주가 되므로 robots.txt·rate-limit 엄격 준수.
3) Extract
   - trafilatura/readability. 필요 도메인만 제한적 커스텀 파서.
4) Deduplicate & Score
   - Plan A와 동일 로직 공유.
5) Store
   - `data_crawl/plan_b/`에 JSONL 저장. 스키마 동일.

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
  "source": "commoncrawl|gdelt|internet_archive|rss|sitemap|youtube|search_api",
  "url": "https://...",                  
  "snapshot_url": "https://web.archive.org/...", 
  "title": "...",
  "text": "...",                         
  "lang": "ko",
  "published_at": "YYYY-MM-DDTHH:MM:SSZ|null",
  "authors": ["..."],
  "discovered_via": {"type": "gdelt", "meta": {"...": "..."}},
  "quality": {"score": 0.0, "reasons": ["len", "lang", "domain"]},
  "dup": {"simhash": "...", "group": "..."},
  "crawl": {"plan": "a|b", "run_id": "2025-11-03-a1", "fetched_at": "..."}
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
- 텍스트 최소 길이/밀도 기준 미달은 드롭 또는 `quality.score` 낮게 설정.
- 스냅샷 링크 보존(가능 시 IA, CC WARC 오프셋)으로 재현성 확보.

---

## 아키텍처(로직 레벨)

컴포넌트
1) Discoverer
   - 입력: 키워드, 날짜 범위, 언어. 출력: URL/스냅샷/비디오ID 후보.
2) Fetcher
   - 정책: robots 준수, 도메인별 동시성/초당 요청 제한, 재시도・백오프.
3) Extractor
   - 도구: trafilatura/readability, 인코딩 정규화, 언어 감지(fasttext/cld3), 날짜 추정.
4) Deduper/Scorer
   - URL 정규화(쿼리 파라미터 화이트리스트), canonical 태그 반영, SimHash/MinHash.
5) Storage
   - JSONL 저장, run_id 단위 롤업, 인덱스(개념): url_norm, published_at, lang, simhash_group.

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
- crawl.plan: "a" | "b" | "both" (실행 대상 선택).
- run_id: 자동 생성(예: YYYYMMDD-hhmm-<plan>) 또는 수동 지정.

---

## 디렉토리 구조(초안)

```
crawl/
  README.md                  # 본 문서
  config/
    keywords.txt             # 키워드 리스트(초기안)
    domains_allowlist.txt    # (선택) 우선 도메인
    params.yaml              # 실행 파라미터(start_date 등)
  plan_a/
    __init__.py
    cli.py                   # CLI 진입점
    config.py                # 설정 로더
    models.py
    utils.py
    pipeline.py
 discovery/               # Common Crawl, GDELT, YouTube discoverers
    forums.py             # 주요 커뮤니티 게시판 Discoverer
    fetch/                   # Common Crawl/IA/라이브 fetcher
    extract/                 # 본문·메타 추출
    dedupe/                  # SimHash 기반 중복 처리
    storage/                 # JSONL writer
data_crawl/
  plan_a/                    # Plan A 산출(JSONL)
  plan_b/                    # Plan B 산출(JSONL)
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
