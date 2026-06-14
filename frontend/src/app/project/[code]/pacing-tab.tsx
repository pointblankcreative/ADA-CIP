"use client";

import { useEffect, useRef, useState } from "react";
import {
  api,
  type PacingResponse,
  type PacingLine,
  type BundleMember,
  type PhaseSummary,
  type UntrackedPlatformSpend,
  type DirectLine,
} from "@/lib/api";
import { Card, KpiCard } from "@/components/card";
import { OscilloscopeCard } from "@/components/oscilloscope-card";
import { PlatformIcon } from "@/components/platform-icon";
import { PacingBadge } from "@/components/pacing-badge";
import { CodeChip, Label } from "@/components/ui";
import {
  formatCurrency,
  formatPercent,
  pacingStatus,
  pacingBarColor,
  pacingColor,
  pacingVar,
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

/**
 * Rejected lines surface in pacing data with bundle_role='rejected'. Pacing
 * treats them as not-parents and not-children: the former parent shows up
 * as a standalone (with the pool budget) and children with NULL budgets get
 * dropped before this row is rendered. We still render the parent's row so
 * the user can Clear the override to revert.
 */
function isRejectedBundleParent(line: PacingLine): boolean {
  // A rejected member is a parent (still has the pool budget) iff its
  // planned_budget is non-zero. Children get filtered out upstream.
  return line.bundle_role === "rejected" && line.planned_budget > 0;
}

export function PacingTab({
  code,
  asOfDate,
  projectEnded = false,
  projectSpend,
  projectBudget,
}: {
  code: string;
  /**
   * When provided, fetch the budget_tracking row for this specific date
   * instead of the most recent one. Used by the Retrospective Mode page
   * (ADAC-51 commit 7). Inline-edit affordances and "as of today" nuances
   * are suppressed in retro mode since the view is point-in-time read-only.
   */
  asOfDate?: string;
  /**
   * A1 (pacing resilience): the project-level verdict the detail page already
   * holds (`computeFlight(project).ended` — i.e. status !== "active" / LANDED).
   * Passed so the tab can de-alarm an ENDED flight independently of per-line
   * flight_end dates, which can be mis-stored (24058: dates a year ahead read
   * every line as not-yet-started). Defaults false so the Retrospective page,
   * which renders <PacingTab> without it, behaves exactly as before.
   */
  projectEnded?: boolean;
  /**
   * The project's true warehouse spend / budget (project.total_spend /
   * net_budget). Used as the source of truth for the Spent / Remaining tiles
   * when line-level spend isn't attributed (so the tab never shows $0 against
   * a flight the Summary tab reports as landed). Undefined in retro mode.
   */
  projectSpend?: number;
  projectBudget?: number;
}) {
  const [data, setData] = useState<PacingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  // line_id hovered in the Pacing Signal orbit — glows the matching row
  const [sigHover, setSigHover] = useState<string | null>(null);

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
   * Update local pacing state after a bundle Confirm / Reject / Clear so
   * the UI reflects the change without refetching. Mirrors the backend's
   * parent vs child rule: parents have non-NULL planned_budget (pool
   * total), children have planned_budget=0 (the API maps NULL to 0).
   *
   * State machine:
   *   - "confirmed" → confirmed_parent / confirmed_child
   *   - "suggested" → suggested_parent / suggested_child   (Clear from any state)
   *   - "rejected"  → every member becomes 'rejected'      (Reject)
   */
  const handleBundleStateChange = (
    bundleId: string,
    newState: "confirmed" | "suggested" | "rejected"
  ) => {
    if (!data) return;
    setData({
      ...data,
      lines: data.lines.map((l: PacingLine) => {
        if (l.bundle_id !== bundleId) return l;
        if (newState === "rejected") {
          return { ...l, bundle_role: "rejected" as const };
        }
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
            <div className="h-3 w-20 rounded bg-surface-sunken" />
            <div className="mt-3 h-7 w-28 rounded bg-surface-sunken" />
          </Card>
        ))}
      </div>
    );
  }

  if (!data) {
    return (
      <Card>
        <p className="text-fg-secondary">
          No pacing data available. Run the pacing engine first.
        </p>
      </Card>
    );
  }

  // AI-070/071/072: honest empty state. With the backend's compute-on-miss
  // replay in place, this only fires when the replay itself was impossible
  // (no media plan / no spend data for the window) — the genuinely-absent
  // case. The API echoes the REQUESTED date, never today.
  if (data.snapshot_missing) {
    return (
      <div className="space-y-6">
        <Card>
          <p className="text-fg">
            No pacing snapshot for this date{asOfDate ? ` — ${asOfDate}` : ""}.
          </p>
          <p className="mt-1 text-xs text-fg-muted">
            {data.earliest_snapshot_date
              ? `Pacing snapshots for this project begin ${data.earliest_snapshot_date}.`
              : "This project has no pacing history yet."}
          </p>
        </Card>
        {/* Untracked warehouse spend can exist even without a snapshot
            (AI-002 test: a project whose pacing engine never ran must not
            show $0 when real spend exists). */}
        {(data.untracked_spend ?? 0) > 0 && (
          <UntrackedSpendCard
            platforms={data.untracked_platforms ?? []}
            total={data.untracked_spend ?? 0}
          />
        )}
        {/* Direct buys exist on the media plan independently of any pacing
            snapshot (they're read straight off media_plan_lines), so surface
            them here too. */}
        {(data.direct_budget ?? 0) > 0 && (
          <DirectBuysCard
            lines={data.direct_lines ?? []}
            directBudget={data.direct_budget ?? 0}
            netBudget={data.net_budget}
          />
        )}
      </div>
    );
  }

  const overallStatus = pacingStatus(data.overall_pacing_percentage);

  // AI-002: untracked spend (platforms with no media plan line) is included
  // in Spent / Remaining (conservative — never overstate remaining budget)
  // but excluded from Overall Pacing (no planned baseline). The `??`
  // fallbacks keep the tab working against a not-yet-redeployed backend.
  const untrackedSpend = data.untracked_spend ?? 0;
  const untrackedPlatforms = data.untracked_platforms ?? [];
  const pacingSpendAllPlatforms =
    data.total_actual_all_platforms ?? data.total_actual_to_date;

  // Finding #1 / A1: an ended flight whose per-line spend never got attributed
  // reads as "$0 / 0.0% / NOT STARTED / AWAITING DATA" — directly
  // contradicting the Summary tab's landed total. Two ways we know a flight
  // has finished:
  //   • projectEnded — the detail page's own verdict (status !== "active" /
  //     LANDED). This is authoritative and INDEPENDENT of per-line dates, so
  //     it catches 24058 whose flight_end values are mis-stored a year ahead
  //     and therefore read as not-yet-started.
  //   • flightEnded — the line-date fallback for callers that don't pass
  //     projectEnded (e.g. the Retrospective page): every line is completed,
  //     or its flight_end is at/behind the as-of date.
  // We treat the zeros as an attribution gap (not a real "nothing ran") only
  // when the flight has ended AND pacing genuinely sees no spend anywhere.
  const flightEnded =
    projectEnded ||
    (data.lines.length > 0 &&
      data.lines.every(
        (l) =>
          l.line_status === "completed" ||
          (l.flight_end != null && l.flight_end <= data.as_of_date)
      ));
  const noLineSpend =
    data.lines.reduce((sum, l) => sum + (l.actual_spend_to_date ?? 0), 0) === 0;
  const unattributedSpend =
    flightEnded &&
    noLineSpend &&
    pacingSpendAllPlatforms === 0 &&
    !data.overall_pacing_percentage;

  // A1: prefer the project's true warehouse spend/budget (passed from the
  // detail page) over the pacing payload when line-level spend is unattributed,
  // so the Spent / Remaining tiles show the real figure the Summary tab reports
  // instead of $0. Falls back to the pacing payload when those props are absent
  // (retro mode) or when there's nothing to correct.
  const spentAllPlatforms =
    unattributedSpend && projectSpend != null && projectSpend > 0
      ? projectSpend
      : pacingSpendAllPlatforms;
  const totalBudget =
    projectBudget != null && projectBudget > 0 ? projectBudget : data.net_budget;

  return (
    <div className="space-y-6">
      {/* Pacing Signal — the campaign's lines in orbit */}
      {data.lines.length > 0 && (
        <OscilloscopeCard
          pacing={data}
          code={code}
          asOfDate={asOfDate}
          onHover={setSigHover}
        />
      )}

      {/* (c) Spending-without-baseline warning: spend is present but the line
          has no pacing baseline (usually a data refresh settling). Surfaced
          here rather than silently dropped from the pacing %. */}
      {(data.lines_without_baseline ?? 0) > 0 && (
        <div
          className="rounded-md px-4 py-2.5 text-[13px]"
          style={{
            borderLeft: "3px solid var(--warn)",
            background: "var(--surface-card)",
            color: "var(--fg)",
          }}
        >
          <span className="font-semibold">
            {data.lines_without_baseline} line
            {(data.lines_without_baseline ?? 0) === 1 ? "" : "s"}
          </span>{" "}
          spending {formatCurrency(data.spend_without_baseline ?? 0)} with no
          pacing baseline, likely a data refresh settling. This spend counts in
          the totals but is excluded from the pacing %. Re-pace to refresh.
        </div>
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KpiCard
          label="Total Budget"
          value={formatCurrency(totalBudget)}
        />
        <KpiCard
          label="Spent to Date"
          value={formatCurrency(spentAllPlatforms)}
          sub={
            // A1: when the figure is the project's real warehouse total (line
            // spend unattributed), the tracked/planned breakdown no longer
            // reconciles — say so plainly instead of "of $0 planned".
            unattributedSpend && spentAllPlatforms > 0
              ? "campaign total (not yet attributed to lines)"
              : untrackedSpend > 0
                ? `${formatCurrency(data.total_actual_to_date)} tracked + ${formatCurrency(untrackedSpend)} untracked`
                : `of ${formatCurrency(data.total_planned_to_date)} planned`
          }
        />
        <KpiCard
          label="Remaining"
          value={formatCurrency(totalBudget - spentAllPlatforms)}
        />
        <KpiCard
          label="Overall Pacing"
          value={unattributedSpend ? "—" : formatPercent(data.overall_pacing_percentage)}
          sub={
            unattributedSpend
              ? "line spend not attributed"
              : untrackedSpend > 0
                ? "tracked media plan lines only"
                : undefined
          }
          // Don't paint an alarming red 0.0% when the zero is an attribution
          // gap rather than a real underspend (Finding #1).
          accent={unattributedSpend ? "text-fg-faint" : pacingColor(overallStatus)}
        />
      </div>

      {/* Finding #1: calm, explicit notice when an ended flight's line-level
          spend hasn't been attributed — so the tab never confidently claims
          "nothing ran" against a flight the Summary tab reports as landed. */}
      {unattributedSpend && <UnattributedSpendNotice />}

      {/* AI-002 / AI-022: spend on platforms with no media plan line. Shown
          so the Pacing tab never silently hides real spend. */}
      {untrackedSpend > 0 && (
        <UntrackedSpendCard
          platforms={untrackedPlatforms}
          total={untrackedSpend}
        />
      )}

      {/* bcdirect: direct buys (is_direct lines) — managed directly, excluded
          from pacing. Budget context only: no pacing %, no over/under alarms. */}
      {(data.direct_budget ?? 0) > 0 && (
        <DirectBuysCard
          lines={data.direct_lines ?? []}
          directBudget={data.direct_budget ?? 0}
          netBudget={data.net_budget}
        />
      )}

      {/* Per-line pacing — grouped by phase when there's more than one. */}
      <PacingLinesSection
        data={data}
        code={code}
        asOfDate={asOfDate}
        referenceDate={data.as_of_date}
        projectEnded={projectEnded}
        sigHover={sigHover}
        onNameUpdate={handleNameUpdate}
        onBundleStateChange={handleBundleStateChange}
      />

      {/* As-of stamp */}
      <div className="flex items-center gap-2">
        <Label>As of {data.as_of_date}</Label>
        {/* AI-070/072: rows computed on demand (no stored snapshot for this
            date) — mirrors diagnostics' cached/just-computed indicator. */}
        {data.replayed && (
          <span
            className="rounded-xs bg-surface-sunken px-1.5 py-0.5 font-mono text-[10px] font-medium uppercase tracking-[0.1em] text-fg-muted"
            title="No stored snapshot exists for this date; these figures were reconstructed on demand from warehouse data."
          >
            Reconstructed
          </span>
        )}
      </div>
    </div>
  );
}

/**
 * Finding #1: a flight that has run its course but whose per-line spend never
 * landed against the media plan lines. Rather than render the resulting zeros
 * as a confident "nothing ran" verdict (which contradicts the Summary tab's
 * landed total), surface a calm, explicit notice. Info-toned — this is a data
 * lineage gap, not an alarm. Mirrors UntrackedSpendCard's tinted-border
 * callout pattern.
 */
function UnattributedSpendNotice() {
  return (
    <Card
      className="border-tint-info"
      style={{
        background: "color-mix(in srgb, var(--info) 6%, var(--surface-card))",
        borderLeft: "3px solid var(--info)",
      }}
    >
      <div className="eyebrow" style={{ color: "var(--info)" }}>
        Line spend not attributed
      </div>
      <p className="mt-2.5 max-w-[640px] text-[12.5px] text-fg-muted">
        This flight has ended, but no line-level spend was recorded against the
        media plan, so the per-line figures below read as $0. If the campaign
        did spend, its total is on the Summary tab and the spend hasn&apos;t
        been attributed to lines yet — re-sync the media plan or re-run pacing
        once attribution lands to populate these rows.
      </p>
    </Card>
  );
}

/**
 * AI-002 / AI-022: warn callout listing platforms with real spend in the
 * data warehouse but no line in the synced media plan. These are not paced
 * (no planned baseline) but ARE included in Spent / Remaining so the budget
 * math never hides real spend.
 */
function UntrackedSpendCard({
  platforms,
  total,
}: {
  platforms: UntrackedPlatformSpend[];
  total: number;
}) {
  return (
    <Card
      className="border-tint-warn"
      style={{
        background: "color-mix(in srgb, var(--warn) 6%, var(--surface-card))",
        borderLeft: "3px solid var(--warn)",
      }}
    >
      <div className="flex items-center justify-between gap-3">
        <div className="eyebrow" style={{ color: "var(--warn)" }}>
          Untracked Spend — no media plan line
        </div>
        <span className="font-mono text-sm font-semibold text-warn">
          {formatCurrency(total)}
        </span>
      </div>
      <p className="mb-3 mt-2.5 max-w-[640px] text-[12.5px] text-fg-muted">
        These platforms have spend in the data warehouse but no line in the
        synced media plan, so they are not paced. Check the media plan sheet
        and re-sync, or confirm this spend is expected.
      </p>
      <div className="flex flex-col gap-2">
        {platforms.map((u) => (
          <div
            key={u.platform_id}
            className="flex items-center justify-between"
          >
            <span className="inline-flex items-center gap-2.5 text-[13px] font-semibold text-fg">
              <PlatformIcon platformId={u.platform_id} size={26} />
              {platformLabel(u.platform_id)}
              {u.first_date && u.last_date && (
                <span className="font-mono text-[10.5px] font-normal text-fg-faint">
                  {formatShortDate(u.first_date)} —{" "}
                  {formatShortDate(u.last_date)}
                </span>
              )}
            </span>
            <span className="font-mono text-[13px] text-warn">
              {formatCurrency(u.spend)}
            </span>
          </div>
        ))}
      </div>
    </Card>
  );
}

/**
 * bcdirect: direct buys (media_plan_lines.is_direct = TRUE) — budgeted lines
 * with no self-serve feed (CTV, DOOH direct, LED truck, transit, …). They're
 * EXCLUDED from pacing (no spend feed to pace against), so this card surfaces
 * them purely as budget CONTEXT: a header total, a compact line list, and a
 * one-line reconciliation strip. Deliberately info-toned (not warn) and
 * carries NO pacing %, NO over/under alarms — mirrors UnattributedSpendNotice's
 * calm tinted-border treatment rather than UntrackedSpendCard's warn tone,
 * because a planned direct buy is expected, not a problem to flag.
 */
function DirectBuysCard({
  lines,
  directBudget,
  netBudget,
}: {
  lines: DirectLine[];
  directBudget: number;
  netBudget: number;
}) {
  // Reconciliation identity surfaced on the strip: net = self-serve + direct.
  // Self-serve is the paced remainder (net minus the direct context), so the
  // strip always reconciles regardless of how much line spend has landed.
  const selfServe = netBudget > 0 ? netBudget - directBudget : 0;
  return (
    <Card
      className="border-tint-info"
      style={{
        background: "color-mix(in srgb, var(--info) 6%, var(--surface-card))",
        borderLeft: "3px solid var(--info)",
      }}
    >
      <div className="flex items-center justify-between gap-3">
        <div className="eyebrow" style={{ color: "var(--info)" }}>
          Direct buys — {formatCurrency(directBudget)}
        </div>
        <span className="font-mono text-[11px] font-normal uppercase tracking-[0.08em] text-fg-faint">
          managed directly, not tracked here
        </span>
      </div>
      {lines.length > 0 && (
        <div className="mt-3 flex flex-col gap-2">
          {lines.map((d, i) => (
            <div
              key={`${d.label}-${i}`}
              className="flex items-center justify-between gap-3"
            >
              <span className="inline-flex items-center gap-2.5 text-[13px] font-semibold text-fg">
                {d.platform && (
                  <PlatformIcon platformId={d.platform} size={26} />
                )}
                {d.label}
              </span>
              <span className="font-mono text-[13px] text-fg-muted">
                {formatCurrency(d.budget)}
              </span>
            </div>
          ))}
        </div>
      )}
      {netBudget > 0 && (
        <p className="mt-3 border-t border-line-soft pt-2.5 font-mono text-[11.5px] text-fg-faint">
          {formatCurrency(netBudget)} total = {formatCurrency(selfServe)}{" "}
          self-serve + {formatCurrency(directBudget)} direct (context)
        </p>
      )}
    </Card>
  );
}

/**
 * Render the line-by-line pacing list. Single-plan projects collapse to a flat
 * list (matches the legacy view); multi-plan projects render a header per
 * phase with its aggregate pacing % and a sub-list of lines beneath it.
 *
 * Lines whose sheet_id doesn't match any returned phase (legacy lines synced
 * before project_media_plans existed) are bucketed under an "Unassigned"
 * group at the bottom rather than being silently dropped.
 */
function PacingLinesSection({
  data,
  code,
  asOfDate,
  referenceDate,
  projectEnded = false,
  sigHover,
  onNameUpdate,
  onBundleStateChange,
}: {
  data: PacingResponse;
  code: string;
  asOfDate?: string;
  /** Pacing as-of date (data.as_of_date) — the reference point for deciding
   *  whether a line's flight has ended (Finding #2). */
  referenceDate?: string;
  /** A1: the project-level ended verdict — forces every line to "ended"
   *  regardless of its own (possibly mis-stored) flight_end date. */
  projectEnded?: boolean;
  /** line_id hovered in the Pacing Signal orbit above. */
  sigHover?: string | null;
  onNameUpdate: (lineId: string, newName: string) => void;
  onBundleStateChange: (
    bundleId: string,
    newState: "confirmed" | "suggested" | "rejected"
  ) => void;
}) {
  const phases = data.phases ?? [];
  const hasMultiplePhases = phases.length > 1;

  if (!hasMultiplePhases) {
    return (
      <div>
        <div className="flex items-center gap-3">
          <Label className="text-fg-secondary">Line-by-Line Pacing</Label>
          <div className="h-px flex-1 bg-line-soft" />
        </div>
        <div className="mt-3 space-y-3">
          {data.lines.map((line) => (
            <LineRow
              key={line.line_id}
              line={line}
              code={code}
              asOfDate={asOfDate}
              referenceDate={referenceDate}
              projectEnded={projectEnded}
              glow={sigHover === line.line_id}
              onNameUpdate={onNameUpdate}
              onBundleStateChange={onBundleStateChange}
            />
          ))}
        </div>
      </div>
    );
  }

  // Multi-phase: bucket lines by sheet_id, fall back to "Unassigned" for
  // legacy lines whose sheet didn't make it into project_media_plans.
  const linesBySheet = new Map<string, PacingLine[]>();
  const unassigned: PacingLine[] = [];
  for (const line of data.lines) {
    if (!line.sheet_id) {
      unassigned.push(line);
      continue;
    }
    const arr = linesBySheet.get(line.sheet_id);
    if (arr) arr.push(line);
    else linesBySheet.set(line.sheet_id, [line]);
  }

  return (
    <div>
      <div className="flex items-center gap-3">
        <Label className="text-fg-secondary">
          Line-by-Line Pacing · {phases.length} phases
        </Label>
        <div className="h-px flex-1 bg-line-soft" />
      </div>
      <div className="mt-3 space-y-6">
        {phases.map((phase, idx) => (
          <PhaseGroup
            key={phase.sheet_id}
            phase={phase}
            phaseNumber={idx + 1}
            lines={linesBySheet.get(phase.sheet_id) ?? []}
            code={code}
            asOfDate={asOfDate}
            referenceDate={referenceDate}
            projectEnded={projectEnded}
            sigHover={sigHover}
            onNameUpdate={onNameUpdate}
            onBundleStateChange={onBundleStateChange}
          />
        ))}
        {unassigned.length > 0 && (
          <div>
            <div className="mb-2 rounded-md border-2 border-line-soft bg-surface-sunken px-4 py-2">
              <div className="label text-[10px]">Unassigned</div>
              <div className="text-[11px] text-fg-muted">
                {unassigned.length} line{unassigned.length === 1 ? "" : "s"}{" "}
                from a sheet that&apos;s no longer registered against this
                project.
              </div>
            </div>
            <div className="space-y-3">
              {unassigned.map((line) => (
                <LineRow
                  key={line.line_id}
                  line={line}
                  code={code}
                  asOfDate={asOfDate}
                  referenceDate={referenceDate}
                  projectEnded={projectEnded}
                  glow={sigHover === line.line_id}
                  onNameUpdate={onNameUpdate}
                  onBundleStateChange={onBundleStateChange}
                />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function PhaseGroup({
  phase,
  phaseNumber,
  lines,
  code,
  asOfDate,
  referenceDate,
  projectEnded = false,
  sigHover,
  onNameUpdate,
  onBundleStateChange,
}: {
  phase: PhaseSummary;
  /** 1-based index used as a fallback label when phase_label is null. */
  phaseNumber: number;
  lines: PacingLine[];
  code: string;
  asOfDate?: string;
  /** Pacing as-of date — reference point for the line-ended check (Finding #2). */
  referenceDate?: string;
  /** A1: project-level ended verdict — forces line rows to "ended". */
  projectEnded?: boolean;
  /** line_id hovered in the Pacing Signal orbit above. */
  sigHover?: string | null;
  onNameUpdate: (lineId: string, newName: string) => void;
  onBundleStateChange: (
    bundleId: string,
    newState: "confirmed" | "suggested" | "rejected"
  ) => void;
}) {
  const status = pacingStatus(phase.pacing_percentage);
  const heading = phase.phase_label ?? `Phase ${phase.display_order ?? phaseNumber}`;
  return (
    <div>
      <div className="mb-3 flex items-center justify-between gap-4 rounded-md border-2 border-line-soft bg-surface-sunken px-4 py-3">
        <div>
          <div className="flex items-center gap-2">
            <div className="text-sm font-bold text-fg">{heading}</div>
            {!phase.is_active && (
              <span className="rounded-xs bg-surface-card px-1.5 py-0.5 font-mono text-[10px] font-medium uppercase tracking-[0.06em] text-fg-muted">
                retired
              </span>
            )}
          </div>
          <div className="mt-0.5 font-mono text-[11px] text-fg-muted">
            {phase.line_count} line{phase.line_count === 1 ? "" : "s"} ·{" "}
            {formatCurrency(phase.planned_budget)} planned
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="text-right">
            <div className="label text-[9.5px]">Spent</div>
            <div className="tnum text-sm font-semibold text-fg">
              {formatCurrency(phase.actual_spend_to_date)}
              <span className="text-fg-faint">
                {" "}
                / {formatCurrency(phase.planned_spend_to_date)}
              </span>
            </div>
          </div>
          <div className={cn("text-right", pacingColor(status))}>
            <div className="label text-[9.5px] opacity-80">Pacing</div>
            <div className="tnum text-sm font-bold">
              {formatPercent(phase.pacing_percentage)}
            </div>
          </div>
        </div>
      </div>
      <div className="space-y-3">
        {lines.length === 0 ? (
          <div className="px-4 py-3 text-xs italic text-fg-muted">
            No active lines in this phase yet.
          </div>
        ) : (
          lines.map((line) => (
            <LineRow
              key={line.line_id}
              line={line}
              code={code}
              asOfDate={asOfDate}
              referenceDate={referenceDate}
              projectEnded={projectEnded}
              glow={sigHover === line.line_id}
              onNameUpdate={onNameUpdate}
              onBundleStateChange={onBundleStateChange}
            />
          ))
        )}
      </div>
    </div>
  );
}


function LineRow({
  line,
  code,
  asOfDate,
  referenceDate,
  projectEnded = false,
  glow = false,
  onNameUpdate,
  onBundleStateChange,
}: {
  line: PacingLine;
  /** Project code, needed for bundle Confirm/Clear API calls. */
  code: string;
  /** Set in retrospective mode — disables interactive bundle buttons. */
  asOfDate?: string;
  /** Pacing as-of date (data.as_of_date). Reference point for deciding
   *  whether this line's flight has ended; falls back to today. */
  referenceDate?: string;
  /** A1: project-level ended verdict (LANDED). When true the line is treated
   *  as ended regardless of its own flight_end — 24058's per-line dates are
   *  mis-stored a year ahead, so the date-based check alone wrongly leaves
   *  the line "active" with a phantom "Nd remaining" countdown. */
  projectEnded?: boolean;
  /** This line is hovered in the Pacing Signal orbit — light the row in
   *  its status colour. */
  glow?: boolean;
  onNameUpdate: (lineId: string, newName: string) => void;
  /** Called after a successful bundle Confirm/Reject/Clear API call so the
   *  parent state updates without a re-fetch. */
  onBundleStateChange: (
    bundleId: string,
    newState: "confirmed" | "suggested" | "rejected"
  ) => void;
}) {
  const isCompleted = line.line_status === "completed";
  const status = pacingStatus(line.pacing_percentage);
  // Finding #2: the API can leave remaining_days / daily_budget_required
  // populated (e.g. "176d remaining · $238/day needed") on a line whose
  // flight has already ended, producing impossible countdowns. Derive the
  // ended state from the line's own end date relative to the pacing as-of
  // date (or today when the payload lacks one) and trust that over the raw
  // counters: a finished flight has zero runway and needs no daily pace.
  const refDate = referenceDate ?? new Date().toISOString().slice(0, 10);
  const lineEnded =
    projectEnded ||
    isCompleted ||
    (line.flight_end != null && line.flight_end < refDate);
  // A1: when the whole project has LANDED, a line the backend still tags
  // "not_started"/"pending" (because its flight_end is mis-stored ahead) would
  // otherwise render an alarming "NOT STARTED" pill. Present the badge as the
  // existing "Completed" state instead — no new vocabulary — and suppress the
  // misleading percentage. The line-date and `completed` paths are unchanged.
  const badgeLineStatus: PacingLine["line_status"] =
    projectEnded && !isCompleted ? "completed" : line.line_status;
  const badgePercentage = projectEnded && !isCompleted ? null : line.pacing_percentage;
  const glowColor =
    line.pacing_percentage == null ? "var(--info)" : pacingVar(status);
  const budgetPct =
    line.planned_budget > 0
      ? (line.actual_spend_to_date / line.planned_budget) * 100
      : 0;

  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState(line.audience_name || "");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Bundle Confirm / Reject / Clear (ADAC-54 follow-up + Reject UX).
  // Disabled in retrospective mode — past snapshots are read-only.
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

  const handleBundleReject = async () => {
    if (!line.bundle_id) return;
    setBundleSaving(true);
    setBundleError(null);
    try {
      await api.bundles.reject(code, line.bundle_id);
      onBundleStateChange(line.bundle_id, "rejected");
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
  const bundleRejected = isRejectedBundleParent(line);
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
    <Card
      className={cn("!p-3 sm:!p-4", isCompleted && "opacity-60")}
      style={
        glow
          ? {
              borderColor: glowColor,
              boxShadow: `0 0 0 1.5px ${glowColor}, 0 0 26px color-mix(in srgb, ${glowColor} 28%, transparent)`,
              transition: "border-color var(--dur-base), box-shadow var(--dur-base)",
            }
          : undefined
      }
    >
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-center gap-3">
          <PlatformIcon platformId={line.platform_id} size={34} />
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5 sm:gap-2">
              <span className="text-sm font-bold text-fg">
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
                    className="w-48 rounded-sm border-2 border-line bg-surface-sunken px-1.5 py-0.5 text-xs text-accent-ink outline-none focus:border-accent disabled:opacity-60"
                    placeholder="Line name..."
                  />
                  {error && (
                    <span className="text-xs text-danger">{error}</span>
                  )}
                </div>
              ) : (
                <>
                  {displayName && (
                    <span className="font-mono text-xs font-medium text-accent-ink">
                      {displayName}
                    </span>
                  )}
                  <button
                    onClick={() => {
                      setEditValue(line.audience_name || displayName);
                      setEditing(true);
                    }}
                    className="text-fg-faint transition-colors hover:text-fg-muted"
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
              {line.line_code && <CodeChip>{line.line_code}</CodeChip>}
              {(bundleParent || bundleRejected) && bundleMemberCount > 0 && (
                <>
                  <span
                    className={cn(
                      "rounded-xs border px-1.5 py-0.5 font-mono text-[10px] font-medium",
                      line.bundle_role === "suggested_parent" &&
                        "border-dashed border-tint-warn bg-tint-warn text-warn",
                      line.bundle_role === "confirmed_parent" &&
                        "border-tint-ok bg-tint-ok text-ok",
                      line.bundle_role === "rejected" &&
                        "border-dashed border-line bg-surface-sunken text-fg-muted"
                    )}
                    title={
                      line.bundle_role === "suggested_parent"
                        ? "Suggested bundle — the media plan's merged Budget cell grouped these lines. Confirm to lock in for this and future syncs, or Reject to treat the parent as a standalone."
                        : line.bundle_role === "confirmed_parent"
                        ? "Confirmed bundle — locked in by user. Clear to revert to the parser's suggestion."
                        : "Rejected bundle — treated as a standalone with the pool budget. Children are hidden from pacing because their budgets were zeroed by the parser. Clear to revert to the parser's suggestion."
                    }
                  >
                    {line.bundle_role === "suggested_parent" && "Suggested "}
                    {line.bundle_role === "rejected" && "Rejected "}
                    Bundle · {bundleMemberCount + 1} lines
                  </span>
                  {/* Confirm / Reject / Clear buttons (ADAC-54 follow-up + Reject UX).
                      Suppressed in retrospective mode — past snapshots
                      are read-only. */}
                  {!asOfDate && line.bundle_role === "suggested_parent" && (
                    <>
                      <button
                        type="button"
                        onClick={handleBundleConfirm}
                        disabled={bundleSaving}
                        className="rounded-xs border border-tint-ok bg-tint-ok px-1.5 py-0.5 font-mono text-[10px] font-semibold text-ok hover:opacity-80 disabled:opacity-50"
                        title="Lock this bundle in. Persists across re-syncs."
                      >
                        {bundleSaving ? "Confirming…" : "Confirm"}
                      </button>
                      <button
                        type="button"
                        onClick={handleBundleReject}
                        disabled={bundleSaving}
                        className="rounded-xs border border-tint-danger bg-tint-danger px-1.5 py-0.5 font-mono text-[10px] font-semibold text-danger hover:opacity-80 disabled:opacity-50"
                        title="Treat the parent line as a standalone with the pool budget. Children are hidden from pacing because the parser zeroed their budgets when it detected the bundle. To restore children with their own budgets, un-merge the source sheet's Budget cells and re-sync."
                      >
                        {bundleSaving ? "Rejecting…" : "Reject"}
                      </button>
                    </>
                  )}
                  {!asOfDate &&
                    (line.bundle_role === "confirmed_parent" ||
                      line.bundle_role === "rejected") && (
                      <button
                        type="button"
                        onClick={handleBundleClear}
                        disabled={bundleSaving}
                        className="rounded-xs border border-line bg-surface-sunken px-1.5 py-0.5 font-mono text-[10px] font-semibold text-fg-secondary hover:bg-surface-up disabled:opacity-50"
                        title="Revert to the parser's suggestion. Next sync re-decides from the spreadsheet."
                      >
                        {bundleSaving ? "Clearing…" : "Clear"}
                      </button>
                    )}
                  {bundleError && (
                    <span className="text-[10px] text-danger" title={bundleError}>
                      Bundle action failed
                    </span>
                  )}
                </>
              )}
            </div>
            <div className="mt-0.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 font-mono text-[11px] text-fg-muted">
              <span>{formatCurrency(line.actual_spend_to_date)} spent</span>
              <span>of {formatCurrency(line.planned_budget)} budget</span>
              {dateRange && <span>{dateRange}</span>}
              {isCompleted ? (
                <span className="text-fg-secondary">
                  Final: {formatPercent(budgetPct)} utilized
                </span>
              ) : lineEnded ? (
                // Finding #2: flight is past its end date but not flagged
                // completed — render "ended" and suppress the impossible
                // countdown / daily-needed a finished flight can't have.
                <span className="text-fg-secondary">ended</span>
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
                className="cursor-help font-mono text-[10px] text-fg-faint"
                title={line.line_id}
              >
                {line.line_id.split("-").pop()}
              </span>
            </div>
          </div>
        </div>
        <div className="flex-shrink-0 self-end sm:self-auto">
          <PacingBadge percentage={badgePercentage} lineStatus={badgeLineStatus} />
        </div>
      </div>

      {/* Progress bar */}
      <div className="mt-3.5">
        <div className="relative h-2.5 w-full overflow-hidden rounded-pill bg-surface-sunken">
          {/* Planned marker at planned % */}
          <div
            className="absolute bottom-0 top-0 z-10 w-0.5 bg-fg-secondary"
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
              "h-full rounded-pill transition-all duration-700 ease-snap",
              isCompleted ? "bg-done" : pacingBarColor(status)
            )}
            style={{ width: `${Math.min(budgetPct, 100)}%` }}
          />
        </div>
        <div className="mt-[5px] flex justify-between font-mono text-[9.5px] text-fg-faint">
          <span>0%</span>
          <span>Budget {formatCurrency(line.planned_budget)}</span>
        </div>
      </div>

      {/* PR 5: bundle members (CBO-style shared budget). Parent row carries
          the pacing signal; the audiences below share this pool. Rejected
          bundles also render the expandable list so the user can see which
          audiences were hidden from pacing — Reject UX. */}
      {(bundleParent || bundleRejected) && bundleMemberCount > 0 && (
        <div className="mt-3 border-t border-line-soft pt-2">
          <button
            onClick={() => setBundleExpanded((v) => !v)}
            className="flex w-full items-center justify-between text-xs text-fg-secondary transition-colors hover:text-fg"
          >
            <span>
              {bundleExpanded ? "Hide" : "Show"} {bundleMemberCount} other{" "}
              {bundleMemberCount === 1 ? "audience" : "audiences"}{" "}
              {bundleRejected
                ? "hidden from pacing (rejected bundle)"
                : "sharing this budget"}
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
                  className="flex items-center gap-2 pl-4 text-xs text-fg-secondary"
                >
                  <span className="h-px w-3 bg-line" aria-hidden />
                  {m.line_code && <CodeChip>{m.line_code}</CodeChip>}
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
