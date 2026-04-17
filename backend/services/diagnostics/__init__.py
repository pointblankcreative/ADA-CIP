"""CIP Diagnostic Signal Engine.

Computes per-campaign health scores from BigQuery data, stores results
in fact_diagnostic_signals, and routes critical alerts through the
existing Slack pipeline.

Two campaign types:
    - Persuasion: Distribution → Attention → Resonance
    - Conversion: Acquisition → Funnel
        (A Quality pillar was originally scoped but is deferred
        pending per-client CRM integration — see
        docs/diagnostics/quality-pillar-deferred.md.)

Spec version: 1.1 (Phase 0 validated 2026-04-11)
"""
