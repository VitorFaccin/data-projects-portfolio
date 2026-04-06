from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AnalysisWindow:
    start_date: str
    end_date: str
