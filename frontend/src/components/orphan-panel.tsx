"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertCircle,
  Calendar,
  DollarSign,
  Eye,
  EyeOff,
  Settings,
  Undo2,
  X,
} from "lucide-react";
import { api, type OrphanProject } from "@/lib/api";
import { Card } from "@/components/card";
import { formatCurrency, formatNumber, platformLabel, cn } from "@/lib/utils";

/**
 * OrphanPanel — Overview-page widget listing project_codes with spend/activity
 * in fact_* tables that don't have a dim_projects row.
 *
 * For each orphan the user can:
 * - Configure → redirect to /admin/projects/new?code=XXXXX (form prefill)
 * - Dismiss   → POST /api/orphan-projects/{code}/dismiss (permanent-until-reversed)
 *
 * The panel collapses itself when there are no visible orphans so it never
 * takes up real estate without payload. Toggle "Show dismissed" to expose
 * previously-dismissed codes for un-dismiss.
 */
export function OrphanPanel() {
  const [orphans, setOrphans] = useState<OrphanProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDismissed, setShowDismissed] = useState(false);
  const [busyCode, setBusyCode] = useState<string | null>(null);

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

  const handleDismiss = async (code: string) => {
    const reason =
      typeof window !== "undefined"
        ? window.prompt(
            `Dismiss project ${code}?\n\nOptional: add a short reason (e.g. "old test account" or "client code mismatch"). Leave blank to dismiss without a note.`,
            ""
          )
        : "";
    // null = user hit cancel; empty string = proceed without reason
    if (reason === null) return;

    setBusyCode(code);
    try {
      await api.orphans.dismiss(code, reason || undefined);
      await load(showDismissed);
    } catch (e) {
      setError(e instanceof Error ? e.message : `Failed to dismiss ${code}`);
    } finally {
      setBusyCode(null);
    }
  };

  const handleUndismiss = async (code: string) => {
    setBusyCode(code);
    try {
      await api.orphans.undismiss(code);
      await load(showDismissed);
    } catch (e) {
      setError(e instanceof Error ? e.message : `Failed to un-dismiss ${code}`);
    } finally {
      setBusyCode(null);
    }
  };

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
        Active spend in these project codes hasn&apos;t been configured in CIP
        yet. Configure to start tracking, or dismiss if it doesn&apos;t belong
        here (historical data, test account, wrong client code, etc).
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
          orphans.map((o) => (
            <OrphanCard
              key={o.project_code}
              orphan={o}
              busy={busyCode === o.project_code}
              onDismiss={handleDismiss}
              onUndismiss={handleUndismiss}
            />
          ))
        )}
      </div>
    </div>
  );
}

interface OrphanCardProps {
  orphan: OrphanProject;
  busy: boolean;
  onDismiss: (code: string) => void;
  onUndismiss: (code: string) => void;
}

function OrphanCard({ orphan: o, busy, onDismiss, onUndismiss }: OrphanCardProps) {
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

      {/* Actions */}
      <div className="mt-4 flex items-center gap-2">
        {o.dismissed ? (
          <button
            onClick={() => onUndismiss(o.project_code)}
            disabled={busy}
            className="flex flex-1 items-center justify-center gap-1.5 rounded-md border border-slate-700 bg-slate-800/50 px-3 py-2 text-xs font-medium text-slate-300 transition-colors hover:bg-slate-700 disabled:opacity-50"
          >
            <Undo2 className="h-3.5 w-3.5" />
            Un-dismiss
          </button>
        ) : (
          <>
            <Link
              href={`/admin/projects/new?code=${o.project_code}`}
              className="flex flex-1 items-center justify-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-xs font-medium text-emerald-400 transition-colors hover:bg-emerald-500/20"
            >
              <Settings className="h-3.5 w-3.5" />
              Configure
            </Link>
            <button
              onClick={() => onDismiss(o.project_code)}
              disabled={busy}
              className="flex items-center justify-center gap-1.5 rounded-md border border-slate-700 bg-slate-800/50 px-3 py-2 text-xs font-medium text-slate-400 transition-colors hover:bg-slate-700 hover:text-slate-200 disabled:opacity-50"
              title="Dismiss — hide this code from the orphan list. Permanent until un-dismissed."
            >
              <X className="h-3.5 w-3.5" />
              Dismiss
            </button>
          </>
        )}
      </div>
    </Card>
  );
}
