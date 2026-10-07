from __future__ import annotations

import argparse
import json

from phase7.c1_insight import AnthropicInsightProvider, CuratedEvidence, generate_insight
from phase7.c3_investigation import (
    build_investigation_prompt,
    collect_context,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--grid-id", type=int, default=4821)
    args = parser.parse_args()

    context = collect_context(args.grid_id)
    c1_evidence = CuratedEvidence(
        as_of=context.as_of,
        summary={"grid_id": context.grid_id, "current": context.current_evidence},
        hotspots=[],
        alerts=context.prior_alerts,
        risk=[context.model_score] if context.model_score else [],
        pipeline_status=context.pipeline_status or {},
    )
    provider = AnthropicInsightProvider()
    print("C1 insight:")
    print(generate_insight(c1_evidence, provider))
    print("\nC3 investigation:")
    print(provider.generate(build_investigation_prompt(context)))
    print("\nC2/C3 collected tool evidence:")
    print(json.dumps(context.__dict__, indent=2, default=str))


if __name__ == "__main__":
    main()
