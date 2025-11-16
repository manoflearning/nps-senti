# JSONL deduplication

`jsonl_dedup.cpp` removes duplicate articles (same content, different IDs/URLs) and can run on a single JSONL or over all JSONL files in a directory.

## Build

From the repo root:

```bash
g++ -std=c++17 -Ipreprocess/include -o preprocess/jsonl_dedup preprocess/jsonl_dedup.cpp
```

## Run

- All JSONL files in a directory (default when no args):
  - If `data_crawl/` exists, uses it and writes to `data_preprocessed/`.
  - Otherwise, if `data_preprocessed/` exists, uses it and writes to `data_preprocessed_dedup/`.
  ```bash
  ./preprocess/jsonl_dedup
  ./preprocess/jsonl_dedup --all /input/dir /output/dir
  ```
- Single file:
  ```bash
  ./preprocess/jsonl_dedup /path/to/input.jsonl            # output → default output dir / <filename>
  ./preprocess/jsonl_dedup /path/to/input.jsonl /path/to/output.jsonl
  ```

## Deduplication logic

- Normalize `text` (or `title` if text is missing): lowercase ASCII and collapse whitespace.
- If available, append normalized URL to the key to avoid collisions on short articles (and for title-only cases).
- Keep the first occurrence of each key; skip the rest. Errors are logged and skipped per line.
