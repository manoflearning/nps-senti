# nps-senti Crawl CLI 사용 가이드

이 문서는 크롤링 파이프라인 실행 방법과 주요 옵션, 예시를 정리합니다.

## 실행 방법

- 기본 실행: `uv run python -m crawl.cli`
- 파라미터 파일 지정: `uv run python -m crawl.cli --params crawl/config/params.yaml`
- 로깅 레벨: `--log-level {DEBUG,INFO,WARNING,ERROR}` (기본: INFO)

환경 변수
- `YOUTUBE_API_KEY`가 설정되어 있으면 YouTube 발견 및 댓글/메타 보강이 활성화됩니다.
- (선택) YouTube 댓글 옵션
  - `YOUTUBE_COMMENTS_PAGES` (기본 5): 페이지 수 제한
  - `YOUTUBE_COMMENTS_INCLUDE_REPLIES` (기본 true): 대댓글 포함
  - `YOUTUBE_COMMENTS_ORDER` (기본 relevance): `relevance` 또는 `time`
  - `YOUTUBE_COMMENTS_TEXT_FORMAT` (기본 html): `html` 또는 `plainText`

## 주요 옵션

- `--only {forums,youtube,gdelt} [...]`
  - 지정한 소스만 실행합니다. 미지정 시 설정(및 각 소스 enabled)에 따라 모두 실행합니다.
- `--forums-sites SITE [...]`
  - forums 실행 시, 지정한 사이트 키만 대상으로 발견을 수행합니다.
  - 지원 예: `dcinside`, `bobaedream`, `mlbpark`, `theqoo`, `ppomppu` (params.yaml에 해당 키가 enabled여야 함)
- `--max-fetch N`
  - 이번 실행에서 fetch 시도 최대 건수를 제한합니다(저장 건수 아님). 빠른 샘플링에 유용합니다.

## 예시

- 포럼만 실행: `uv run python -m crawl.cli --only forums`
- 포럼 중 디시/엠팍만: `uv run python -m crawl.cli --only forums --forums-sites dcinside mlbpark`
- 유튜브만 실행: `uv run python -m crawl.cli --only youtube`
- GDELT만 10건만 시도: `uv run python -m crawl.cli --only gdelt --max-fetch 10`
- 포럼+유튜브 조합, 최대 50건: `uv run python -m crawl.cli --only forums youtube --max-fetch 50`
- 커스텀 params로 실행: `uv run python -m crawl.cli --params my/params.yaml --only forums`

## 출력 형식

- 저장 디렉터리: `params.yaml`의 `output.root` (기본: `data_crawl`)
- 포럼: `forum_{site}.jsonl` (예: `forum_dcinside.jsonl`)
- 기타 소스: `{source}.jsonl` (예: `gdelt.jsonl`, `youtube.jsonl`)

## 팁

- 포럼 사이트/보드는 `crawl/config/params.yaml`의 `sources.forums`에서 관리합니다.
- 시간 범위, 키워드, 품질 기준 등도 `params.yaml`에서 설정합니다.
- 로컬 테스트 시에는 `--max-fetch`로 빠르게 동작을 확인한 뒤 제한을 제거하고 전체 실행하세요.
