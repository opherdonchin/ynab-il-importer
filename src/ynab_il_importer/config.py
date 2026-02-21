from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ProjectPaths:
    raw_dir: Path = Path("data/raw")
    derived_dir: Path = Path("data/derived")

    @property
    def bank_raw(self) -> Path:
        return self.raw_dir / "bank.xls"

    @property
    def card_raw(self) -> Path:
        return self.raw_dir / "card.xlsx"

    @property
    def ynab_raw(self) -> Path:
        return self.raw_dir / "ynab_register.csv"

    @property
    def bank_normalized(self) -> Path:
        return self.derived_dir / "bank_normalized.csv"

    @property
    def card_normalized(self) -> Path:
        return self.derived_dir / "card_normalized.csv"

    @property
    def ynab_normalized(self) -> Path:
        return self.derived_dir / "ynab_normalized.csv"

    @property
    def matched_pairs(self) -> Path:
        return self.derived_dir / "matched_pairs.csv"

    @property
    def fingerprint_groups(self) -> Path:
        return self.derived_dir / "fingerprint_groups.csv"
