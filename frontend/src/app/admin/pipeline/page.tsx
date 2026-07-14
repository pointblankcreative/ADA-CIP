"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  Play,
  RotateCw,
  Loader2,
  CheckCircle2,
  XCircle,
  Clock,
} from "lucide-react";
import {
  api,
  type PlatformFreshness,
  type IngestionRun,
  type TransformationStatus,
} from "@/lib/api";
import { Card, KpiCard } from "@/components/card";
import { Label } from "@/components/ui";
import { TH_CLS } from "@/lib/chart-theme";
import { cn, platformLabel } from "@/lib/utils";

function timeAgo(dateStr: string | null): string {
  if (!dateStr) return "—";
  const d = new Date(dateStr);
  const now = new Date();
  const hours = Math.floor((now.getTime() - d.getTime()) / (1000 * 60 * 60));
  if (hours < 1) return "< 1 hour ago";
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatElapsed(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}m ${String(s).padStart(2, "0")}s`;
}

/* ── Full History Backfill: fire-and-poll (ADA 1215990005858989) ──
 * The POST runs the whole job inside one open synchronous request that can
 * take several minutes; the browser may time out on it even though the server
 * finishes fine. So we fire the POST WITHOUT awaiting it on the UI path and
 * poll ingestion_log (via transformationStatus) for the true outcome. NO fake
 * percentage — honest running / success / failed / stalled only. */
type BackfillPhase = "starting" | "running" | "success" | "failed" | "stalled";
type BackfillState = {
  clickedAtMs: number; // client clock at button press (elapsed + startup fallback)
  /** A polled run counts as "ours" iff Date.parse(started_at) > floorMs. Seeded
   *  from the prior transform_full row's start (server-vs-server, clock-skew
   *  proof); falls back to clickedAtMs − 60s; lowered on a 409 to attach to the
   *  already-running run. */
  floorMs: number;
  phase: BackfillPhase;
  startedAt: string | null; // this run's run_started_at once observed (server ISO)
  finishedAt: string | null;
  rows: number;
  error: string | null;
};

const POLL_MS = 3000;
const STARTUP_GRACE_MS = 45_000; // the 'running' row should appear within seconds
// Consistent with the backend FULL_TRANSFORM_ACTIVE_WINDOW_MIN (60 min): while
// the server still treats the run as active we show "running"; past that it is
// stale/dead server-side, so we surface "may have stalled". Tune on staging
// after watching a real backfill.
const STALE_MS = 60 * 60 * 1000;

export default function PipelinePage() {
  const [freshness, setFreshness] = useState<PlatformFreshness[]>([]);
  const [runs, setRuns] = useState<IngestionRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [runningAction, setRunningAction] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<Record<string, unknown> | null>(null);
  const [backfill, setBackfill] = useState<BackfillState | null>(null);
  const [nowMs, setNowMs] = useState<number>(() => Date.now());

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [f, r] = await Promise.all([
        api.admin.dataFreshness(),
        api.admin.ingestionLog(10),
      ]);
      setFreshness(f.platforms);
      setRuns(r.runs);
    } catch { /* ignore */ } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  async function runAction(name: string, fn: () => Promise<Record<string, unknown>>) {
    setRunningAction(name);
    setLastResult(null);
    try {
      const result = await fn();
      setLastResult(result);
      await fetchData();
    } catch (err: unknown) {
      setLastResult({ error: err instanceof Error ? err.message : "Unknown error" });
    } finally {
      setRunningAction(null);
    }
  }

  // Kicks off the full backfill. Synchronous state set FIRST (before any await)
  // so the button disables immediately — a fast double-click is impossible.
  function startBackfill() {
    if (backfill && (backfill.phase === "starting" || backfill.phase === "running")) {
      return; // already in flight — belt-and-braces on top of the disabled button
    }
    const clickedAtMs = Date.now();
    setLastResult(null);
    setBackfill({
      clickedAtMs,
      floorMs: clickedAtMs - 60_000, // provisional; refined once baseline lands
      phase: "starting",
      startedAt: null,
      finishedAt: null,
      rows: 0,
      error: null,
    });
    setNowMs(clickedAtMs);
    void beginBackfill(clickedAtMs);
  }

  async function beginBackfill(clickedAtMs: number) {
    // Correlation baseline: capture the prior transform_full row's start so a
    // stale prior row is never mistaken for this run. Best-effort — the
    // client-clock floor is the fallback.
    let floorMs = clickedAtMs - 60_000;
    try {
      const s = await api.admin.transformationStatus("full");
      if (s.found && s.started_at) floorMs = Date.parse(s.started_at);
    } catch { /* keep the client-clock floor */ }
    setBackfill((prev) => (prev ? { ...prev, floorMs } : prev));

    // Fire the long SYNCHRONOUS POST. Do NOT await it on the UI path and do NOT
    // AbortController/cancel it — the request must run to completion server-side
    // (that is what does the work). Swallow the eventual client-side rejection
    // (timeout / "Failed to fetch"); the poll is the single source of truth.
    void api.admin
      .runFullBackfill()
      .then((r) => {
        if (r.conflict && r.run?.started_at) {
          // A run is already in flight (409). Attach our poll to it by lowering
          // the floor just below its start, rather than surfacing an error.
          const attach = Date.parse(r.run.started_at) - 1;
          setBackfill((prev) => (prev ? { ...prev, floorMs: attach } : prev));
        }
      })
      .catch(() => { /* intentionally swallowed — see the poll loop */ });
  }

  // Poll loop: one interval while the run is not yet terminal. Reads everything
  // from prev state so there are no stale closures; swallows transient status
  // errors (retry next tick) so a blip never manufactures a false failure.
  useEffect(() => {
    const phase = backfill?.phase;
    if (!phase || phase === "success" || phase === "failed") return;

    const id = setInterval(async () => {
      const t = Date.now();
      setNowMs(t);
      let s: TransformationStatus;
      try {
        s = await api.admin.transformationStatus("full");
      } catch {
        return; // transient hiccup — retry next tick
      }
      setBackfill((prev) => {
        if (!prev || prev.phase === "success" || prev.phase === "failed") return prev;
        const isOurs =
          s.found && s.started_at != null && Date.parse(s.started_at) > prev.floorMs;
        if (!isOurs) {
          // Our run's row hasn't appeared yet (POST still spinning up, or it
          // never reached the server). Surface "stalled" past the grace window.
          if (t - prev.clickedAtMs > STARTUP_GRACE_MS) {
            return { ...prev, phase: "stalled" };
          }
          return prev;
        }
        const base = { ...prev, startedAt: s.started_at!, rows: s.rows ?? 0 };
        if (s.status === "success") {
          return { ...base, phase: "success", finishedAt: s.finished_at ?? null, error: null };
        }
        if (s.status === "failed") {
          return {
            ...base,
            phase: "failed",
            finishedAt: s.finished_at ?? null,
            error: s.error ?? "Import failed (no error detail recorded).",
          };
        }
        // status === 'running' — flip to stalled once past the server window.
        const runElapsed = t - Date.parse(s.started_at!);
        return { ...base, phase: runElapsed > STALE_MS ? "stalled" : "running" };
      });
    }, POLL_MS);
    return () => clearInterval(id);
  }, [backfill?.phase]);

  // Smooth 1s elapsed ticker while a run is in flight (the poll only ticks
  // nowMs every few seconds).
  useEffect(() => {
    const phase = backfill?.phase;
    if (!phase || phase === "success" || phase === "failed") return;
    const id = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(id);
  }, [backfill?.phase]);

  // Refresh the freshness + ingestion-log tables once the run resolves.
  const backfillPhase = backfill?.phase;
  useEffect(() => {
    if (backfillPhase === "success" || backfillPhase === "failed") {
      fetchData();
    }
  }, [backfillPhase, fetchData]);

  const backfillActive =
    !!backfill && (backfill.phase === "starting" || backfill.phase === "running");
  const actionsDisabled = !!runningAction || backfillActive;

  const totalRows = freshness.reduce((s, p) => s + p.total_rows, 0);

  return (
    <div className="mx-auto max-w-[1100px]">
      <Link
        href="/admin"
        className="mb-3 inline-flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-[0.08em] text-fg-muted transition-colors hover:text-fg"
      >
        <ArrowLeft className="h-3.5 w-3.5" /> Admin
      </Link>

      <h1 className="text-xl font-extrabold tracking-tight text-fg">Pipeline Control</h1>
      <p className="mt-1 text-sm text-fg-muted">
        Run transformations, check data freshness, and monitor ingestion.
      </p>

      {/* Actions */}
      <div className="mt-6 flex flex-wrap gap-3">
        <button
          onClick={() => runAction("daily", api.admin.dailyRun)}
          disabled={actionsDisabled}
          className="flex items-center gap-2 rounded-sm border-2 border-accent bg-accent px-4 py-2.5 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover disabled:opacity-50"
        >
          {runningAction === "daily" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
          Run Daily Pipeline
        </button>
        <button
          onClick={startBackfill}
          disabled={actionsDisabled}
          className="flex items-center gap-2 rounded-sm border-2 border-line px-4 py-2.5 text-sm font-bold text-fg transition-colors hover:border-line-strong disabled:opacity-50"
        >
          {backfillActive ? <Loader2 className="h-4 w-4 animate-spin" /> : <RotateCw className="h-4 w-4" />}
          Full History Backfill
        </button>
      </div>

      {/* Full History Backfill progress (honest running/done/failed/stalled) */}
      {backfill && (
        <Card className="mt-4">
          <Label className="mb-2">Full History Backfill</Label>
          {backfill.phase === "starting" && (
            <p className="flex items-center gap-2 text-sm text-fg-secondary">
              <Loader2 className="h-4 w-4 animate-spin text-fg-muted" />
              Starting the full history backfill… hang on.
            </p>
          )}
          {backfill.phase === "running" && (
            <p className="flex items-center gap-2 text-sm text-fg-secondary">
              <Loader2 className="h-4 w-4 animate-spin text-fg-muted" />
              Full history backfill running —{" "}
              <span className="tnum font-mono">
                {formatElapsed(
                  nowMs - (backfill.startedAt ? Date.parse(backfill.startedAt) : backfill.clickedAtMs)
                )}
              </span>{" "}
              elapsed. This reloads all history and can take several minutes. You can
              leave this page; it keeps running on the server.
            </p>
          )}
          {backfill.phase === "success" && (
            <p className="flex items-center gap-2 text-sm text-ok">
              <CheckCircle2 className="h-4 w-4" />
              Full history backfill complete — {backfill.rows.toLocaleString()} rows loaded
              {backfill.finishedAt
                ? ` at ${new Date(backfill.finishedAt).toLocaleTimeString()}`
                : ""}
              .
            </p>
          )}
          {backfill.phase === "failed" && (
            <div className="text-sm">
              <p className="flex items-center gap-2 text-danger">
                <XCircle className="h-4 w-4" />
                Full history backfill failed.
              </p>
              {backfill.error && (
                <pre className="mt-2 max-h-40 overflow-auto whitespace-pre-wrap rounded-sm bg-surface-sunken p-2 font-mono text-xs text-fg-secondary">
                  {backfill.error}
                </pre>
              )}
            </div>
          )}
          {backfill.phase === "stalled" && (
            <p className="flex items-center gap-2 text-sm text-warn">
              <Clock className="h-4 w-4" />
              This run has been going for a while with no result — it may have stalled.
              Check the pipeline logs and the Recent Ingestion Runs table below before
              re-running.
            </p>
          )}
        </Card>
      )}

      {/* Action result (Daily Pipeline) */}
      {lastResult && (
        <Card className="mt-4">
          <Label className="mb-2">Last Run Result</Label>
          <pre className="max-h-60 overflow-auto whitespace-pre-wrap font-mono text-xs text-fg-secondary">
            {JSON.stringify(lastResult, null, 2)}
          </pre>
        </Card>
      )}

      {/* KPIs */}
      <div className="mt-6 grid gap-4 sm:grid-cols-3">
        <KpiCard
          label="Platforms Tracked"
          value={String(freshness.length)}
        />
        <KpiCard
          label="Total Rows"
          value={totalRows.toLocaleString()}
        />
        <KpiCard
          label="Recent Runs"
          value={String(runs.length)}
        />
      </div>

      {/* Data Freshness */}
      <Card className="mt-6 overflow-x-auto p-0">
        <div className="border-b border-line-soft px-4 py-3">
          <Label className="text-fg-secondary">Data Freshness</Label>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-line-soft text-left">
              <th className={TH_CLS}>Platform</th>
              <th className={TH_CLS}>Latest Data</th>
              <th className={TH_CLS}>Last Load</th>
              <th className={cn(TH_CLS, "text-right")}>Days</th>
              <th className={cn(TH_CLS, "text-right")}>Rows</th>
              <th className={cn(TH_CLS, "text-center")}>Status</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-fg-muted">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                </td>
              </tr>
            )}
            {freshness.map((p) => {
              const stale =
                p.latest_loaded_at &&
                (new Date().getTime() - new Date(p.latest_loaded_at).getTime()) >
                  36 * 60 * 60 * 1000;
              return (
                <tr
                  key={p.platform_id}
                  className="border-b border-line-soft transition-colors hover:bg-surface-sunken"
                >
                  <td className="px-4 py-3 font-medium text-fg">
                    {platformLabel(p.platform_id)}
                  </td>
                  <td className="tnum px-4 py-3 font-mono text-fg-muted">
                    {p.latest_data_date ?? "—"}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-fg-muted">
                    {timeAgo(p.latest_loaded_at)}
                  </td>
                  <td className="tnum px-4 py-3 text-right font-mono text-fg">
                    {p.total_days.toLocaleString()}
                  </td>
                  <td className="tnum px-4 py-3 text-right font-mono text-fg">
                    {p.total_rows.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-center">
                    {stale ? (
                      <span className="inline-flex items-center gap-1 font-mono text-xs uppercase text-warn">
                        <Clock className="h-3 w-3" /> Stale
                      </span>
                    ) : (
                      <CheckCircle2 className="mx-auto h-4 w-4 text-ok" />
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>

      {/* Ingestion Log */}
      <Card className="mt-6 overflow-x-auto p-0">
        <div className="border-b border-line-soft px-4 py-3">
          <Label className="text-fg-secondary">Recent Ingestion Runs</Label>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-line-soft text-left">
              <th className={TH_CLS}>Pipeline</th>
              <th className={TH_CLS}>Mode</th>
              <th className={TH_CLS}>Status</th>
              <th className={cn(TH_CLS, "text-right")}>Rows</th>
              <th className={TH_CLS}>Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.length === 0 && !loading && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-fg-muted">
                  No ingestion runs yet.
                </td>
              </tr>
            )}
            {runs.map((r, i) => (
              <tr
                key={r.run_id ?? i}
                className="border-b border-line-soft transition-colors hover:bg-surface-sunken"
              >
                <td className="px-4 py-3 font-medium text-fg">{r.pipeline_name}</td>
                <td className="px-4 py-3 font-mono text-xs text-fg-muted">{r.mode}</td>
                <td className="px-4 py-3">
                  {r.status === "success" ? (
                    <span className="inline-flex items-center gap-1 font-mono text-xs uppercase text-ok">
                      <CheckCircle2 className="h-3 w-3" /> success
                    </span>
                  ) : r.status === "error" ? (
                    <span className="inline-flex items-center gap-1 font-mono text-xs uppercase text-danger">
                      <XCircle className="h-3 w-3" /> error
                    </span>
                  ) : (
                    <span className="font-mono text-xs text-fg-muted">{r.status}</span>
                  )}
                </td>
                <td className="tnum px-4 py-3 text-right font-mono text-fg">
                  {(r.rows_processed ?? 0).toLocaleString()}
                </td>
                <td className="px-4 py-3 font-mono text-xs text-fg-muted">
                  {r.started_at ? new Date(r.started_at).toLocaleString() : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
