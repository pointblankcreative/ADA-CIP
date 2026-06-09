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
import { api, type PlatformFreshness, type IngestionRun } from "@/lib/api";
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

export default function PipelinePage() {
  const [freshness, setFreshness] = useState<PlatformFreshness[]>([]);
  const [runs, setRuns] = useState<IngestionRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [runningAction, setRunningAction] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<Record<string, unknown> | null>(null);

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
          disabled={!!runningAction}
          className="flex items-center gap-2 rounded-sm border-2 border-accent bg-accent px-4 py-2.5 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover disabled:opacity-50"
        >
          {runningAction === "daily" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
          Run Daily Pipeline
        </button>
        <button
          onClick={() => runAction("full", () => api.admin.runTransformation("full"))}
          disabled={!!runningAction}
          className="flex items-center gap-2 rounded-sm border-2 border-line px-4 py-2.5 text-sm font-bold text-fg transition-colors hover:border-line-strong disabled:opacity-50"
        >
          {runningAction === "full" ? <Loader2 className="h-4 w-4 animate-spin" /> : <RotateCw className="h-4 w-4" />}
          Full History Backfill
        </button>
      </div>

      {/* Action result */}
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
