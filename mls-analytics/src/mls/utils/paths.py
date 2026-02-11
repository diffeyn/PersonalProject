from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import date

def repo_root() -> Path:
    # .../src/mls/utils/paths.py -> utils -> mls -> src -> repo
    return Path(__file__).resolve().parents[3]

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

@dataclass(frozen=True)
class DataPaths:
    root: Path

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def raw(self) -> Path:
        return self.data / "raw"

    @property
    def interim(self) -> Path:
        return self.data / "interim"

    @property
    def processed(self) -> Path:
        return self.data / "processed"

    def raw_bucket(self, kind: str) -> Path:
        return self.raw / kind  # matches/players/teams

    def run_date(self) -> str:
        return date.today().isoformat()