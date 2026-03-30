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
    <div>
      <Link
        href="/admin"
        className="inline-flex items-center gap-1.5 text-sm text-slate-400 hover:text-white transition-colors mb-3"
      >
        <ArrowLeft className="h-4 w-4" /> Admin
      </Link>

      <h1 className="text-xl font-semibold text-white">Pipeline Control</h1>
      <p className="mt-1 text-sm text-slate-400">
        Run transformations, check data freshness, and monitor ingestion.
      </p>

      {/* Actions */}
      <div className="mt-6 flex flex-wrap gap-3">
        <button
          onClick={() => runAction("daily", api.admin.dailyRun)}
          disabled={!!runningAction}
          className="flex items-center gap-2 rounded-md bg-brand-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-brand-500 disabled:opacity-50 transition-colors"
        >
          {runningAction === "daily" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
          Run Daily Pipeline
        </button>
        <button
          onClick={() => runAction("full", () => api.admin.runTransformation("full"))}
          disabled={!!runningAction}
          className="flex items-center gap-2 rounded-md border border-slate-700 px-4 py-2.5 text-sm font-medium text-slate-300 hover:bg-slate-800 disabled:opacity-50 transition-colors"
        >
          {runningAction === "full" ? <Loader2 className="h-4 w-4 animate-spin" /> : <RotateCw className="h-4 w-4" />}
          Full History Backfill
        </button>
      </div>

      {/* Action result */}
      {lastResult && (
        <Card className="mt-4">
          <h3 className="text-xs font-medium uppercase tracking-wider text-slate-500 mb-2">
            Last Run Result
          </h3>
          <pre className="text-xs text-slate-300 whitespace-pre-wrap overflow-auto max-h-60">
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
        <div className="px-4 py-3 border-b border-slate-800">
          <h2 className="text-sm font-medium text-white">Data Freshness</h2>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider">
              <th className="px-4 py-3">Platform</th>
              <th className="px-4 py-3">Latest Data</th>
              <th className="px-4 py-3">Last Load</th>
              <th className="px-4 py-3 text-right">Days</th>
              <th className="px-4 py-3 text-right">Rows</th>
              <th className="px-4 py-3 text-center">Status</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-slate-500">
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
                  className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors"
                >
                  <td className="px-4 py-3 text-white font-medium">
                    {platformLabel(p.platform_id)}
                  </td>
                  <td className="px-4 py-3 text-slate-400 tabular-nums">
                    {p.latest_data_date ?? "—"}
                  </td>
                  <td className="px-4 py-3 text-slate-400 text-xs">
                    {timeAgo(p.latest_loaded_at)}
                  </td>
                  <td className="px-4 py-3 text-right text-white tabular-nums">
                    {p.total_days.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-right text-white tabular-nums">
                    {p.total_rows.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-center">
                    {stale ? (
                      <span className="inline-flex items-center gap-1 text-xs text-amber-400">
                        <Clock className="h-3 w-3" /> Stale
                      </span>
                    ) : (
                      <CheckCircle2 className="mx-auto h-4 w-4 text-emerald-400" />
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
        <div className="px-4 py-3 border-b border-slate-800">
          <h2 className="text-sm font-medium text-white">Recent Ingestion Runs</h2>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider">
              <th className="px-4 py-3">Pipeline</th>
              <th className="px-4 py-3">Mode</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3 text-right">Rows</th>
              <th className="px-4 py-3">Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.length === 0 && !loading && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-slate-500">
                  No ingestion runs yet.
                </td>
              </tr>
            )}
            {runs.map((r, i) => (
              <tr
                key={r.run_id ?? i}
                className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors"
              >
                <td className="px-4 py-3 text-white">{r.pipeline_name}</td>
                <td className="px-4 py-3 text-slate-400 text-xs">{r.mode}</td>
                <td className="px-4 py-3">
                  {r.status === "success" ? (
                    <span className="inline-flex items-center gap-1 text-xs text-emerald-400">
                      <CheckCircle2 className="h-3 w-3" /> success
                    </span>
                  ) : r.status === "error" ? (
                    <span className="inline-flex items-center gap-1 text-xs text-red-400">
                      <XCircle className="h-3 w-3" /> error
                    </span>
                  ) : (
                    <span className="text-xs text-slate-400">{r.status}</span>
                  )}
                </td>
                <td className="px-4 py-3 text-right text-white tabular-nums">
                  {(r.rows_processed ?? 0).toLocaleString()}
                </td>
                <td className="px-4 py-3 text-xs text-slate-400">
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
