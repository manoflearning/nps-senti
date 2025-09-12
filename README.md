# National Pension Sentiment Analysis

## Directory Structure [DRAFT]

- `crawl/` fetches & parses sources; writes raw items to DB/data/raw.
- `preprocess/` cleans text, masks PII, and deduplicates → model-ready data.
- `ml/` vectorizes (TF-IDF), trains baseline, runs batch inference; heavy deps live here; saves to data/artifacts.
- `topics/` keyword tagging plus light topic models (NMF/LDA).
- `analytics/` time-series aggregates, Z-score anomalies, event correlation.
- `viz/` composes a single static Plotly dashboard for sharing.
- `core/` thin shared infra (config, SQLite/FTS5, I/O, logging).
- `data/` is the contract: raw/, processed/, artifacts/; exchange via files/DB only.
- `cli.py` wires stages end-to-end with subcommands; expose `nps-senti` console script.
- Data/DB path configurable via `NPS_DATA_DIR` (defaults to `./data`).
- `reports/` stores publishable outputs (e.g., dashboard.html).

```bash
national-pension-sentiment-analysis/
├─ pyproject.toml
├─ README.md
├─ src/
│  └─ nps_senti/                         # Source package (installed from src/)
│     ├─ cli.py                          # Single entrypoint; routes subcommands
│     ├─ core/                           # Shared infra (config / SQLite+FTS5 / I/O / logging)
│     │  ├─ config.py
│     │  ├─ db.py                        # DB connection + schema init + light CRUD
│     │  ├─ io.py
│     │  └─ log.py
│     ├─ crawl/                          # Crawling only (requests/parse/store)
│     │  ├─ sources/                     # Site/API adapters
│     │  └─ run.py                       # Incremental pipeline → data/raw & DB
│     ├─ preprocess/                     # Make raw text model-ready
│     │  ├─ clean.py                     # Normalize + PII mask
│     │  └─ dedup.py                     # Exact/near-dup removal
│     ├─ ml/                             # Features / train / infer (heavy deps live here if used)
│     │  ├─ featurize.py                 # TF-IDF (char n-grams)
│     │  ├─ train.py                     # Baseline train (SVM/LogReg); save artifacts
│     │  ├─ infer.py                     # Batch inference → sentiments table
│     │  └─ export_onnx.py               # (Optional) ONNX export
│     ├─ topics/                         # Issue tagging & topic modeling
│     │  ├─ keywords.py
│     │  └─ models.py
│     ├─ analytics/                      # Aggregations / time-series / anomalies / event correlation
│     │  ├─ trends.py
│     │  ├─ events.py
│     │  └─ correlate.py
│     └─ viz/                            # Plotly → single static HTML dashboard
│        ├─ charts.py
│        └─ dashboard.py
├─ data/                                 # Pipeline outputs (contract)
│  ├─ raw/                               # Crawled JSONL(.gz)
│  ├─ processed/                         # Cleaned/ready
│  └─ artifacts/                         # Models/vectorizers/metrics
├─ reports/                              # Shareable deliverables
│  └─ dashboard/                         # dashboard.html
└─ scripts/                              # Ops helpers
   ├─ bootstrap_db.py                    # Initialize schema
   └─ crontab.example                    # Scheduling examples
```
