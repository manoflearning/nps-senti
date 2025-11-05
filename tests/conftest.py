import sys
from pathlib import Path

# Ensure the 'crawl' package (repo root/crawl) is importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
