"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { AlertCircle, Eye, EyeOff, Settings } from "lucide-react";
import { api, type OrphanProject } from "@/lib/api";
import { Card } from "@/components/card";
import { formatCurrency, formatNumber, platformLabel, cn } from "@/lib/utils";

/**
 * OrphanPanel — Overview-page widget listing project_codes with spend/activity
 * in fact_* tables that don't have a dim_projects row.
 *
 * The only action here is Configure → redirect to
 * /admin/projects/new?code=XXXXX (form prefill).
 *
 * Suppression is deliberately NOT a UI action. To set a code aside you add a
 * row to the `dismissed_orphans` control table in BigQuery (level = 'dismissed'
 * hides it from the active panel but keeps it under "Show dismissed";
 * level = 'archived' hides it everywhere). That way nothing can be suppressed
 * by accident. The panel collapses itself when there's nothing to show.
 */
export function OrphanPanel() {
  const [orphans, setOrphans] = useState<OrphanProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDismissed, setShowDismissed] = useState(false);

  const load = async (includeDismissed: boolean) => {
    setLoading(true);
    try {
      const data = await api.orphans.list(includeDismissed);
      setOrphans(data.orphans);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load orphans");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load(showDismissed);
  }, [showDismissed]);

  // Hide the entire panel when there's nothing to show.
  const hasOrphans = orphans.length > 0;
  if (!loading && !hasOrphans && !showDismissed && !error) {
    return null;
  }

  return (
    <div className="mt-8">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <AlertCircle className="h-4 w-4 text-amber-400" />
          <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-400">
            Unconfigured Spend
          </h2>
          {hasOrphans && (
            <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-400">
              {orphans.length}
            </span>
          )}
        </div>
        <button
          onClick={() => setShowDismissed((v) => !v)}
          className="flex items-center gap-1.5 text-xs text-slate-500 transition-colors hover:text-slate-300"
        >
          {showDismissed ? (
            <>
              <EyeOff className="h-3.5 w-3.5" />
              Hide dismissed
            </>
          ) : (
            <>
              <Eye className="h-3.5 w-3.5" />
              Show dismissed
            </>
          )}
        </button>
      </div>

      <p className="mt-1 text-xs text-slate-500">
        Active spend in these project codes hasn&apos;t been configured in ADA
        yet. Configure to start tracking. To set a code aside, add it to the{" "}
        <span className="font-mono text-slate-400">dismissed_orphans</span>{" "}
        table in BigQuery (dismissed = hidden here, archived = hidden
        everywhere).
      </p>

      {error && (
        <div className="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
          {error}
        </div>
      )}

      <div className="mt-3 grid gap-3 lg:grid-cols-2 xl:grid-cols-3">
        {loading ? (
          Array.from({ length: 3 }).map((_, i) => (
            <Card key={i} className="animate-pulse">
              <div className="h-4 w-32 rounded bg-slate-700" />
              <div className="mt-3 h-6 w-24 rounded bg-slate-700" />
              <div className="mt-4 h-2 w-full rounded bg-slate-700" />
            </Card>
          ))
        ) : !hasOrphans ? (
          <p className="col-span-full text-sm text-slate-500">
            {showDismissed
              ? "No dismissed orphans."
              : "No unconfigured spend detected."}
          </p>
        ) : (
          orphans.map((o) => <OrphanCard key={o.project_code} orphan={o} />)
        )}
      </div>
    </div>
  );
}

interface OrphanCardProps {
  orphan: OrphanProject;
}

function OrphanCard({ orphan: o }: OrphanCardProps) {
  const topPlatforms = o.by_platform.slice(0, 4);
  const extraPlatforms = o.by_platform.length - topPlatforms.length;

  return (
    <Card
      className={cn(
        "transition-colors",
        o.dismissed && "opacity-60 border-slate-800/60"
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-slate-300">
              {o.project_code}
            </span>
            {o.dismissed ? (
              <span className="rounded-full border border-slate-700 bg-slate-800/50 px-2 py-0.5 text-[10px] font-medium text-slate-500">
                Dismissed
              </span>
            ) : (
              <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-400">
                Unconfigured
              </span>
            )}
          </div>
          <p className="mt-2 text-lg font-semibold tabular-nums text-white">
            {formatCurrency(o.total_spend)}
          </p>
          <p className="text-xs text-slate-500">
            {formatNumber(o.total_rows)} rows
            {o.first_date && o.last_date && (
              <>
                {" · "}
                {o.first_date}
                {o.first_date !== o.last_date && ` → ${o.last_date}`}
              </>
            )}
          </p>
        </div>
      </div>

      {/* Platform breakdown */}
      <div className="mt-3 space-y-1.5">
        {topPlatforms.map((p) => (
          <div
            key={p.platform_id}
            className="flex items-center justify-between text-xs"
          >
            <span className="text-slate-400">{platformLabel(p.platform_id)}</span>
            <span className="tabular-nums text-slate-500">
              {formatCurrency(p.spend)}
            </span>
          </div>
        ))}
        {extraPlatforms > 0 && (
          <p className="text-xs text-slate-600">
            + {extraPlatforms} more platform{extraPlatforms > 1 ? "s" : ""}
          </p>
        )}
      </div>

      {o.dismissed && o.dismissed_reason && (
        <div className="mt-3 rounded border border-slate-800 bg-slate-900/50 p-2 text-xs text-slate-500">
          <span className="font-semibold text-slate-400">Reason:</span>{" "}
          {o.dismissed_reason}
        </div>
      )}

      {/* The only action is Configure. Suppression is managed in BigQuery. */}
      {!o.dismissed && (
        <div className="mt-4 flex items-center gap-2">
          <Link
            href={`/admin/projects/new?code=${o.project_code}`}
            className="flex flex-1 items-center justify-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-xs font-medium text-emerald-400 transition-colors hover:bg-emerald-500/20"
          >
            <Settings className="h-3.5 w-3.5" />
            Configure
          </Link>
        </div>
      )}
    </Card>
  );
}
