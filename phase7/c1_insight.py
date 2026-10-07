from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Protocol


@dataclass(frozen=True)
class CuratedEvidence:
    """Evidence already reduced to the public API/ML response boundary."""

    as_of: str
    summary: dict[str, object]
    hotspots: list[dict[str, object]]
    alerts: list[dict[str, object]]
    risk: list[dict[str, object]]
    pipeline_status: dict[str, object]

    def __post_init__(self) -> None:
        if not self.as_of:
            raise ValueError("as_of is required")
        if not isinstance(self.summary, dict):
            raise TypeError("summary must be a dictionary")
        if not isinstance(self.pipeline_status, dict):
            raise TypeError("pipeline_status must be a dictionary")


class InsightProvider(Protocol):
    def generate(self, prompt: str) -> str:
        """Generate an explanation from the supplied curated prompt."""


def build_prompt(evidence: CuratedEvidence) -> str:
    """Build a bounded prompt without reading the warehouse or raw files."""

    return (
        "You are a network operations analyst. Use only the curated evidence "
        "below. Separate observed evidence from interpretation. Do not claim "
        "confirmed congestion or a confirmed network fault. Activity and risk "
        "are investigation signals. If evidence is missing, say insufficient "
        "evidence. Reporting AS_OF is "
        f"{evidence.as_of}.\n\n"
        f"SUMMARY:\n{evidence.summary}\n\n"
        f"HOTSPOTS:\n{evidence.hotspots}\n\n"
        f"ALERTS:\n{evidence.alerts}\n\n"
        f"RISK:\n{evidence.risk}\n\n"
        f"PIPELINE STATUS:\n{evidence.pipeline_status}\n"
    )


class OfflineInsightProvider:
    """Deterministic provider used until an approved Claude key is available."""

    def generate(self, prompt: str) -> str:
        if "Reporting AS_OF is " not in prompt:
            raise ValueError("Prompt is missing the reporting AS_OF")
        return (
            "Evidence summary is available for the reported AS_OF. "
            "Review the ranked hotspots, alerts, risk scores, and pipeline "
            "status before taking action. These outputs are investigation "
            "signals, not a congestion diagnosis or confirmed network fault."
        )


class AnthropicInsightProvider:
    """Claude provider that reads the key only from the process environment."""

    def __init__(
        self,
        model: str = "claude-3-5-haiku-latest",
        max_tokens: int = 700,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens

    def generate(self, prompt: str) -> str:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not configured in this process"
            )
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise RuntimeError(
                "The anthropic package is required for live C1-C3 requests"
            ) from exc

        client = Anthropic(api_key=api_key)
        response = client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=(
                "Use only the supplied curated evidence. Separate evidence "
                "from interpretation and report insufficient evidence."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        text_blocks = [
            block.text
            for block in response.content
            if getattr(block, "type", None) == "text"
        ]
        if not text_blocks:
            raise RuntimeError("Claude returned no text content")
        return "\n".join(text_blocks)


def generate_insight(
    evidence: CuratedEvidence,
    provider: InsightProvider | None = None,
) -> str:
    """Generate an insight through an injectable provider."""

    selected_provider = provider or OfflineInsightProvider()
    return selected_provider.generate(build_prompt(evidence))
