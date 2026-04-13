"""CIP Diagnostic Signal Engine.

Computes per-campaign health scores from BigQuery data, stores results
in fact_diagnostic_signals, and routes critical alerts through the
existing Slack pipeline.

Two campaign types:
    - Persuasion: Distribution → Attention → Resonance
    - Conversion: Acquisition → Funnel → Quality

Spec version: 1.1 (Phase 0 validated 2026-04-11)
"""
