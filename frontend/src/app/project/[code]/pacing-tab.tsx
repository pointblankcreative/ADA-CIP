"use client";

import { useEffect, useRef, useState } from "react";
import { api, type PacingResponse, type PacingLine, type BundleMember } from "@/lib/api";
import { Card, KpiCard } from "@/components/card";
import { OscilloscopeCard } from "@/components/oscilloscope-card";
import { PlatformIcon } from "@/components/platform-icon";
import { PacingBadge } from "@/components/pacing-badge";
import {
  formatCurrency,
  formatPercent,
  pacingStatus,
  pacingBarColor,
  pacingColor,
  platformLabel,
  cn,
} from "@/lib/utils";

function formatShortDate(iso: string | null): string {
  if (!iso) return "";
  const d = new Date(iso + "T00:00:00");
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

/**
 * Label precedence (PR 5): audience_name (most specific) → line_code + channel
 * → platform + channel → channel → platform. Falls back through the options
 * so we always produce something readable, even for thinly-described lines.
 */
function lineDisplayName(line: {
  audience_name?: string | null;
  line_code?: string | null;
  channel_category?: string | null;
  platform_id?: string | null;
}): string {
  if (line.audience_name) return line.audience_name;
  if (line.line_code && line.channel_category) {
    return `${line.line_code} · ${line.channel_category}`;
  }
  if (line.channel_category && line.platform_id) {
    return `${platformLabel(line.platform_id)} · ${line.channel_category}`;
  }
  if (line.channel_category) return line.channel_category;
  if (line.platform_id) return platformLabel(line.platform_id);
  return "Line";
}

function isBundleParent(line: PacingLine): boolean {
  return (
    line.bundle_role === "suggested_parent" ||
    line.bundle_role === "confirmed_parent"
  );
}

export function PacingTab({
  code,
  asOfDate,
}: {
  code: string;
  /**
   * When provided, fetch the budget_tracking row for this specific date
   * instead of the most recent one. Used by the Retrospective Mode page
   * (ADAC-51 commit 7). Inline-edit affordances and "as of today" nuances
   * are suppressed in retro mode since the view is point-in-time read-only.
   */
  asOfDate?: string;
}) {
  const [data, setData] = useState<PacingResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.pacing
      .get(code, asOfDate)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [code, asOfDate]);

  const handleNameUpdate = (lineId: string, newName: string) => {
    if (!data) return;
    setData({
      ...data,
      lines: data.lines.map((l: PacingLine) =>
        l.line_id === lineId ? { ...l, audience_name: newName } : l
      ),
    });
  };

  /**
   * Update local pacing state after a bundle Confirm/Clear so the UI
   * reflects the change without refetching. Mirrors the backend's parent
   * vs child rule: parents have non-NULL planned_budget (pool total),
   * children have planned_budget=0 (the API maps NULL to 0). Apply the
   * matching confirmed or suggested role accordingly.
   */
  const handleBundleStateChange = (
    bundleId: string,
    newState: "confirmed" | "suggested"
  ) => {
    if (!data) return;
    setData({
      ...data,
      lines: data.lines.map((l: PacingLine) => {
        if (l.bundle_id !== bundleId) return l;
        const isChild = l.planned_budget === 0;
        const newRole =
          newState === "confirmed"
            ? isChild
              ? "confirmed_child"
              : "confirmed_parent"
            : isChild
              ? "suggested_child"
              : "suggested_parent";
        return { ...l, bundle_role: newRole };
      }),
    });
  };

  if (loading) {
    return (
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i} className="animate-pulse">
            <div className="h-3 w-20 rounded bg-slate-700" />
            <div className="mt-3 h-7 w-28 rounded bg-slate-700" />
          </Card>
        ))}
      </div>
    );
  }

  if (!data) {
    return (
      <Card>
        <p className="text-slate-400">
          No pacing data available. Run the pacing engine first.
        </p>
      </Card>
    );
  }

  const overallStatus = pacingStatus(data.overall_pacing_percentage);

  return (
    <div className="space-y-6">
      {/* Oscilloscope health card */}
      {data.lines.length > 0 && (
        <OscilloscopeCard pacing={data} code={code} />
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KpiCard
          label="Total Budget"
          value={formatCurrency(data.net_budget)}
        />
        <KpiCard
          label="Spent to Date"
          value={formatCurrency(data.total_actual_to_date)}
          sub={`of ${formatCurrency(data.total_planned_to_date)} planned`}
        />
        <KpiCard
          label="Remaining"
          value={formatCurrency(
            data.net_budget - data.total_actual_to_date
          )}
        />
        <KpiCard
          label="Overall Pacing"
          value={formatPercent(data.overall_pacing_percentage)}
          accent={pacingColor(overallStatus)}
        />
      </div>

      {/* Per-line pacing */}
      <div>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
          Line-by-Line Pacing
        </h3>
        <div className="mt-3 space-y-3">
          {data.lines.map((line) => (
            <LineRow
              key={line.line_id}
              line={line}
              code={code}
              asOfDate={asOfDate}
              onNameUpdate={handleNameUpdate}
              onBundleStateChange={handleBundleStateChange}
            />
          ))}
        </div>
      </div>

      {/* Blocking chart visualization */}
      <div>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
          As of {data.as_of_date}
        </h3>
      </div>
    </div>
  );
}

function LineRow({
  line,
  code,
  asOfDate,
  onNameUpdate,
  onBundleStateChange,
}: {
  line: PacingLine;
  /** Project code, needed for bundle Confirm/Clear API calls. */
  code: string;
  /** Set in retrospective mode — disables interactive bundle buttons. */
  asOfDate?: string;
  onNameUpdate: (lineId: string, newName: string) => void;
  /** Called after a successful bundle Confirm/Clear API call so the
   *  parent state updates without a re-fetch. */
  onBundleStateChange: (
    bundleId: string,
    newState: "confirmed" | "suggested"
  ) => void;
}) {
  const isCompleted = line.line_status === "completed";
  const status = pacingStatus(line.pacing_percentage);
  const budgetPct =
    line.planned_budget > 0
      ? (line.actual_spend_to_date / line.planned_budget) * 100
      : 0;

  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState(line.audience_name || "");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Bundle Confirm / Clear (ADAC-54 follow-up). Disabled in retrospective
  // mode — past snapshots are read-only.
  const [bundleSaving, setBundleSaving] = useState(false);
  const [bundleError, setBundleError] = useState<string | null>(null);

  const handleBundleConfirm = async () => {
    if (!line.bundle_id) return;
    setBundleSaving(true);
    setBundleError(null);
    try {
      await api.bundles.confirm(code, line.bundle_id);
      onBundleStateChange(line.bundle_id, "confirmed");
    } catch (e) {
      setBundleError(e instanceof Error ? e.message : String(e));
    } finally {
      setBundleSaving(false);
    }
  };

  const handleBundleClear = async () => {
    if (!line.bundle_id) return;
    setBundleSaving(true);
    setBundleError(null);
    try {
      await api.bundles.clearOverride(code, line.bundle_id);
      onBundleStateChange(line.bundle_id, "suggested");
    } catch (e) {
      setBundleError(e instanceof Error ? e.message : String(e));
    } finally {
      setBundleSaving(false);
    }
  };

  useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [editing]);

  // PR 5: use the unified label precedence so bundles and standalones share
  // the same naming rules.
  const displayName = line.audience_name || lineDisplayName(line);
  const dateRange =
    line.flight_start && line.flight_end
      ? `${formatShortDate(line.flight_start)} — ${formatShortDate(line.flight_end)}`
      : null;
  const bundleParent = isBundleParent(line);
  const bundleMemberCount = line.bundle_members?.length ?? 0;
  const [bundleExpanded, setBundleExpanded] = useState(false);

  const handleSave = async () => {
    const trimmed = editValue.trim();
    if (!trimmed || trimmed === line.audience_name) {
      setEditing(false);
      setEditValue(line.audience_name || "");
      setError(null);
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await api.admin.updateMediaPlanLine(line.line_id, {
        audience_name: trimmed,
      });
      onNameUpdate(line.line_id, trimmed);
      setEditing(false);
    } catch (err) {
      setError("Failed to update line name. Please try again.");
      setEditValue(line.audience_name || "");
    } finally {
      setSaving(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleSave();
    if (e.key === "Escape") {
      setEditValue(line.audience_name || "");
      setEditing(false);
    }
  };

  return (
    <Card className={cn("!p-3 sm:!p-4", isCompleted && "opacity-60")}>
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3 min-w-0">
          <PlatformIcon platformId={line.platform_id} />
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5 sm:gap-2">
              <span className="text-sm font-medium text-white">
                {platformLabel(line.platform_id)}
              </span>
              {editing ? (
                <div className="flex flex-col gap-1">
                  <input
                    ref={inputRef}
                    value={editValue}
                    onChange={(e) => setEditValue(e.target.value)}
                    onBlur={handleSave}
                    onKeyDown={handleKeyDown}
                    disabled={saving}
                    className="rounded border border-slate-600 bg-slate-800 px-1.5 py-0.5 text-xs text-blue-300 outline-none focus:border-blue-500 w-48 disabled:opacity-60"
                    placeholder="Line name..."
                  />
                  {error && (
                    <span className="text-xs text-red-400">{error}</span>
                  )}
                </div>
              ) : (
                <>
                  {displayName && (
                    <span className="text-xs font-medium text-blue-400">
                      {displayName}
                    </span>
                  )}
                  <button
                    onClick={() => {
                      setEditValue(line.audience_name || displayName);
                      setEditing(true);
                    }}
                    className="text-slate-600 hover:text-slate-400 transition-colors"
                    aria-label="Edit line name"
                    title="Edit line name"
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      viewBox="0 0 16 16"
                      fill="currentColor"
                      className="h-3 w-3"
                    >
                      <path d="M13.488 2.513a1.75 1.75 0 0 0-2.475 0L3.22 10.306a1 1 0 0 0-.258.42l-.97 3.232a.5.5 0 0 0 .616.617l3.232-.97a1 1 0 0 0 .42-.258l7.793-7.793a1.75 1.75 0 0 0 0-2.475l-.565-.566Z" />
                    </svg>
                  </button>
                </>
              )}
              {line.line_code && (
                <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[10px] text-slate-400">
                  {line.line_code}
                </span>
              )}
              {bundleParent && bundleMemberCount > 0 && (
                <>
                  <span
                    className={cn(
                      "rounded border px-1.5 py-0.5 text-[10px] font-medium",
                      line.bundle_role === "suggested_parent"
                        ? "border-dashed border-amber-500/40 bg-amber-500/10 text-amber-300"
                        : "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
                    )}
                    title={
                      line.bundle_role === "suggested_parent"
                        ? "Suggested bundle — the media plan's merged Budget cell grouped these lines. Confirm to lock in for this and future syncs."
                        : "Confirmed bundle — locked in by user. Clear to revert to the parser's suggestion."
                    }
                  >
                    {line.bundle_role === "suggested_parent" ? "Suggested " : ""}
                    Bundle · {bundleMemberCount + 1} lines
                  </span>
                  {/* Confirm / Clear buttons (ADAC-54 follow-up).
                      Suppressed in retrospective mode — past snapshots
                      are read-only. */}
                  {!asOfDate && line.bundle_role === "suggested_parent" && (
                    <button
                      type="button"
                      onClick={handleBundleConfirm}
                      disabled={bundleSaving}
                      className="rounded border border-emerald-500/40 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] font-medium text-emerald-300 hover:bg-emerald-500/20 disabled:opacity-50"
                      title="Lock this bundle in. Persists across re-syncs."
                    >
                      {bundleSaving ? "Confirming…" : "Confirm"}
                    </button>
                  )}
                  {!asOfDate && line.bundle_role === "confirmed_parent" && (
                    <button
                      type="button"
                      onClick={handleBundleClear}
                      disabled={bundleSaving}
                      className="rounded border border-slate-600 bg-slate-800/50 px-1.5 py-0.5 text-[10px] font-medium text-slate-300 hover:bg-slate-800 disabled:opacity-50"
                      title="Revert to the parser's suggestion. Next sync re-decides from the spreadsheet."
                    >
                      {bundleSaving ? "Clearing…" : "Clear"}
                    </button>
                  )}
                  {bundleError && (
                    <span
                      className="text-[10px] text-red-400"
                      title={bundleError}
                    >
                      Bundle action failed
                    </span>
                  )}
                </>
              )}
            </div>
            <div className="mt-0.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs text-slate-500">
              <span>{formatCurrency(line.actual_spend_to_date)} spent</span>
              <span>of {formatCurrency(line.planned_budget)} budget</span>
              {dateRange && <span>{dateRange}</span>}
              {isCompleted ? (
                <span className="text-slate-400">
                  Final: {formatPercent(budgetPct)} utilized
                </span>
              ) : (
                <>
                  {line.remaining_days > 0 && (
                    <span>{line.remaining_days}d remaining</span>
                  )}
                  {line.daily_budget_required != null &&
                    line.daily_budget_required > 0 && (
                      <span>
                        {formatCurrency(line.daily_budget_required)}/day needed
                      </span>
                    )}
                </>
              )}
              <span
                className="font-mono text-[10px] text-slate-600 cursor-help"
                title={line.line_id}
              >
                {line.line_id.split("-").pop()}
              </span>
            </div>
          </div>
        </div>
        <div className="flex-shrink-0 self-end sm:self-auto">
          <PacingBadge percentage={line.pacing_percentage} lineStatus={line.line_status} />
        </div>
      </div>

      {/* Progress bar */}
      <div className="mt-3">
        <div className="relative h-3 w-full overflow-hidden rounded-full bg-slate-800">
          {/* Planned marker at planned % */}
          <div
            className="absolute top-0 bottom-0 w-0.5 bg-slate-500 z-10"
            style={{
              left: `${Math.min(
                (line.planned_spend_to_date / Math.max(line.planned_budget, 1)) * 100,
                100
              )}%`,
            }}
          />
          {/* Actual bar */}
          <div
            className={cn(
              "h-full rounded-full transition-all duration-700",
              isCompleted ? "bg-slate-600" : pacingBarColor(status)
            )}
            style={{ width: `${Math.min(budgetPct, 100)}%` }}
          />
        </div>
        <div className="mt-1 flex justify-between text-[10px] text-slate-600">
          <span>0%</span>
          <span>Budget: {formatCurrency(line.planned_budget)}</span>
        </div>
      </div>

      {/* PR 5: bundle members (CBO-style shared budget). Parent row carries
          the pacing signal; the audiences below share this pool. */}
      {bundleParent && bundleMemberCount > 0 && (
        <div className="mt-3 border-t border-slate-800/60 pt-2">
          <button
            onClick={() => setBundleExpanded((v) => !v)}
            className="flex w-full items-center justify-between text-xs text-slate-400 hover:text-slate-200 transition-colors"
          >
            <span>
              {bundleExpanded ? "Hide" : "Show"} {bundleMemberCount} other{" "}
              {bundleMemberCount === 1 ? "audience" : "audiences"} sharing this
              budget
            </span>
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 16 16"
              fill="currentColor"
              className={cn(
                "h-3 w-3 transition-transform",
                bundleExpanded && "rotate-180"
              )}
            >
              <path d="M8 11 3 6h10z" />
            </svg>
          </button>
          {bundleExpanded && (
            <ul className="mt-2 space-y-1">
              {line.bundle_members.map((m: BundleMember) => (
                <li
                  key={m.line_id}
                  className="flex items-center gap-2 pl-4 text-xs text-slate-400"
                >
                  <span className="h-px w-3 bg-slate-700" aria-hidden />
                  {m.line_code && (
                    <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[10px] text-slate-400">
                      {m.line_code}
                    </span>
                  )}
                  <span className="truncate">
                    {m.audience_name ||
                      lineDisplayName({
                        line_code: m.line_code,
                        channel_category: line.channel_category,
                        platform_id: line.platform_id,
                      })}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </Card>
  );
}
