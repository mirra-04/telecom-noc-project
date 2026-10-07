# Phase 7 — Claude-Assisted Network Operations

Phase 7 adds Claude above the existing API and ML layers. C4 establishes
repository rules before any Claude API calls are made. C1 now has an offline
foundation that can be tested without an Anthropic key.

## C4 artifacts

- Root `CLAUDE.md`: architecture, terminology, data-grain, geography, and
  `AS_OF` rules.
- `phase7/repository_map.md`: verified repository map and data flow.
- `phase7/proposed_missing_tests.md`: follow-up validation ideas.

## C1 offline foundation

[`c1_insight.py`](./c1_insight.py) accepts only curated summary, hotspot,
alert, risk, and pipeline-status responses. It builds a bounded prompt and
uses a deterministic offline provider by default. No warehouse connection,
raw extract, or API key is used.

Run its tests from the repository root:

```powershell
& ".\.venv\Scripts\python.exe" -m pytest phase7\tests -q
```

The live [`AnthropicInsightProvider`](./c1_insight.py) reads
`ANTHROPIC_API_KEY` only from the current process environment. It does not
print, persist, or include the key in prompts.

After installing the SDK in the same PowerShell session where the key is set:

```powershell
& ".\.venv\Scripts\python.exe" -m pip install anthropic
& ".\.venv\Scripts\python.exe" -m phase7.run_live_validation --grid-id 4821
```

This sends only curated API/ML evidence and runs C1 plus the C3 investigation
prompt. The C2 tool loop remains the local evidence collector and logs its
tool calls; no raw warehouse data is sent.

## C2 offline tool-using assistant

[`c2_assistant.py`](./c2_assistant.py) defines the C2 tool surface and an
offline deterministic agent loop. It calls pipeline status before situation
claims, records every tool call, reports failed tools, and cites tool names in
the answer. It uses existing curated services and persisted ML outputs; it
does not read raw source data.

## C3 offline investigation foundation

[`c3_investigation.py`](./c3_investigation.py) collects current activity,
summarized history, prior alerts, model output, location, and pipeline status
through existing service boundaries. It preserves failures and creates
structured current/history/uncertainty sections. The checklist is documented
in [`context_engineering_checklist.md`](./context_engineering_checklist.md).

## Secret handling

The Anthropic key must never be committed to the repository or placed in
source code. Before a future C1–C3 run, set it only in the current PowerShell
session:

```powershell
$env:ANTHROPIC_API_KEY = "paste-your-key-here"
```

Clear it when finished:

```powershell
Remove-Item Env:ANTHROPIC_API_KEY
```

Do not paste the key into chat. C4 does not need the key. I will ask for
explicit confirmation immediately before any command or code path uses it.
