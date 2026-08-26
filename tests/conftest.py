import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "quant"))

# 테스트는 사용자 환경변수의 영향을 받으면 안 된다
for k in list(os.environ):
    if k.startswith("MDFEED_"):
        del os.environ[k]
