# Raw Data Schema: Press Releases

## File
- Path: `data/raw/press_releases.jsonl`
- Format: one JSON object per line, UTF-8 encoded, newline-delimited

## Fields
- `source` *(string, required)*: crawler source identifier. For press releases, the value is `"nps_press_release"`.
- `item_id` *(string, required)*: stable identifier extracted from the source URL (e.g., `seq` query parameter).
- `url` *(string, required)*: absolute URL to the public detail page.
- `title` *(string, required)*: headline text from the listing or detail view.
- `content` *(string, required)*: HTML-stripped article body with preserved paragraph breaks.
- `published_at` *(string, required)*: publication timestamp in ISO8601 UTC (e.g., `2023-11-02T00:00:00Z`).
- `fetched_at` *(string, required)*: crawler retrieval timestamp in ISO8601 UTC.
- `attachments` *(array of strings, required)*: absolute URLs to attachment assets (PDF/HWP/etc.). Empty if none are advertised.
- `raw_html` *(string, required)*: raw HTML snapshot of the detail page for debugging/auditing.

## Guarantees
- Records are sorted by `(published_at, item_id)` when written.
- Duplicate `item_id` entries are suppressed across runs.
- Fields marked required are always present; optional future fields must tolerate unknown keys.

## Example
```json
{
  "source": "nps_press_release",
  "item_id": "202311",
  "url": "https://www.nps.or.kr/jsppage/news/pressrelease/view.jsp?seq=202311",
  "title": "국민연금공단, 새로운 제도 발표",
  "content": "국민연금공단은 새로운 서비스를 도입한다.\n상세 내용은 첨부파일을 참고하세요.",
  "published_at": "2023-11-02T00:00:00Z",
  "fetched_at": "2024-01-05T09:00:00Z",
  "attachments": [
    "https://www.nps.or.kr/files/press/202311/policy.pdf"
  ],
  "raw_html": "<html>...</html>"
}
```
