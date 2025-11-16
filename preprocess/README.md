# GDELT deduplication

`gdelt_dedup.cpp` removes duplicate GDELT articles (same content, different IDs/URLs) and writes a clean JSONL file.

## Build

From the repo root:

```bash
g++ -std=c++17 -Ipreprocess/include -o preprocess/gdelt_dedup preprocess/gdelt_dedup.cpp
```

## Run

Defaults read `data_crawl/gdelt.jsonl` and write `data_preprocessed/gdelt.jsonl`:

```bash
./preprocess/gdelt_dedup
```

You can override paths:

```bash
./preprocess/gdelt_dedup /path/to/input.jsonl /path/to/output.jsonl
```

## Deduplication logic

- Normalize `text` (or `title` if text is missing): lowercase ASCII and collapse whitespace.
- If available, append normalized URL to the key to avoid collisions on very short articles.
- Keep the first occurrence of each key; skip the rest.
