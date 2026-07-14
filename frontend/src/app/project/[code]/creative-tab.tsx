"use client";

/**
 * Creative tab — the Call Sheet. The rotation is the page.
 *
 * Question-led structure, ported from the Claude Design prototype
 * (tab-creative.jsx):
 *   1. Who earns the next dollar?         → the call + ranked rotation
 *   2. Is the winner winning everywhere?  → creative × platform matrix
 *   3. What goes to the client?           → report totals
 *   4. Does the site keep what media buys? → GA4 funnel
 *
 * The campaign objective declares the primary KPI (✱). It sets the
 * rank; attention metrics (hook, watch-through) explain it. Verdicts
 * come from lib/creative.ts — the engine, not this view.
 */
import { useEffect, useMemo, useState } from "react";
import { Layers, Pencil, Plug } from "lucide-react";
import {
  api,
  API_BASE,
  type AdPerformanceResponse,
  type AudienceMatrixResponse,
  type BenchmarkResponse,
  type CreativeMatrixCell,
  type CreativeMatrixResponse,
  type CreativeRotationResponse,
  type CreativeVariantResponse,
  type GA4PerformanceResponse,
  type MatrixAudience,
  type ObjectiveType,
  type PerformanceResponse,
  type RotationCoverage,
  type RotationCreative,
  type RotationWindow,
} from "@/lib/api";
import {
  buildBenches,
  buildCreativeCall,
  formatMoney,
  formatRate,
  formatTimes,
  isUnresolvedVariant,
  judgeCreative,
  lensesFor,
  pickReportConversions,
  primaryKpi,
  rankCreatives,
  rotationImbalance,
  VOLUME_MIN_IMPRESSIONS,
  type CreativeBenches,
  type JudgedCreative,
  type LensId,
  type QuartileBench,
} from "@/lib/creative";
import {
  CoverageLine,
  CreativeVerdictChip,
  LensSwitch,
  MatrixCell,
  QuartileBar,
  SectionHead,
  Spark,
  VerdictWord,
  WarnStrip,
} from "@/components/perf/primitives";
import { PlacementFrame } from "@/components/perf/placement-frame";
import { Card } from "@/components/card";
import { Glossary } from "@/components/glossary";
import { Btn, Eyebrow, Label } from "@/components/ui";
import { PlatformIcon } from "@/components/platform-icon";
import { SyncStatus } from "@/components/sync-status";
import {
  cn,
  formatCurrencyCompact,
  formatNumber,
  formatNumberCompact,
  platformLabel,
} from "@/lib/utils";

/* ── Empty state — calm, not an error ────────────────────────────── */

function AdLevelAwaiting() {
  return (
    <Card className="flex flex-col items-center gap-3.5 px-6 py-14 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-md border-[1.5px] border-tint-info bg-tint-info">
        <Layers className="h-[22px] w-[22px] text-info" />
      </div>
      <div>
        <h3 className="text-lg font-bold text-fg">
          Ad-level data not available for this campaign yet
        </h3>
        <p className="mx-auto mt-2 max-w-[460px] text-[13.5px] leading-relaxed text-fg-muted">
          Creative reporting needs ad-level sync confirmed for this campaign.
          Platform totals keep flowing in Pacing meanwhile: nothing is lost,
          this page just won&apos;t guess.
        </p>
      </div>
    </Card>
  );
}

/* ── Alias — inline-renameable creative name ─────────────────────── */

function AliasName({
  name,
  onRename,
  showNameCta = false,
}: {
  name: string;
  onRename: (value: string) => void;
  /** #19: render a plain "Name this creative" trigger alongside the pencil,
   *  for unresolved variants that need naming (reuses the same editor). */
  showNameCta?: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(name);

  // Keep the draft in sync when the optimistic rename lands.
  useEffect(() => setValue(name), [name]);

  if (editing) {
    return (
      <input
        autoFocus
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onBlur={() => {
          setEditing(false);
          onRename(value);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            setEditing(false);
            onRename(value);
          }
          if (e.key === "Escape") {
            setValue(name);
            setEditing(false);
          }
        }}
        className="w-full min-w-0 rounded-sm border-2 border-line bg-surface-sunken px-2 py-0.5 text-sm font-bold text-accent-ink outline-none focus:border-accent"
      />
    );
  }
  return (
    <span className="flex min-w-0 items-center gap-1.5">
      <span className="truncate text-[15px] font-extrabold tracking-[-0.01em] text-fg">
        {name}
      </span>
      <button
        onClick={() => setEditing(true)}
        className="flex-shrink-0 text-fg-faint transition-colors hover:text-fg-muted"
        title="Rename this creative. The alias persists across syncs."
        aria-label={`Rename ${name}`}
      >
        <Pencil className="h-3 w-3" />
      </button>
      {showNameCta && (
        <button
          onClick={() => setEditing(true)}
          className="flex-shrink-0 whitespace-nowrap font-mono text-[9px] font-bold tracking-[0.06em] text-accent-ink transition-colors hover:text-fg"
        >
          Name this creative
        </button>
      )}
    </span>
  );
}

/* ── Attention funnel stages, shaped by the objective ────────────── */

interface FunnelStage {
  label: string;
  value: number | null;
  bench?: QuartileBench;
  format: (v: number) => string;
  /** This stage sets the rank (the objective's primary KPI). */
  primary?: boolean;
  /** Shown for completeness on awareness flights. */
  notKpi?: boolean;
  /** Honest-gap copy when the value is null. */
  na?: string;
}

function buildStages(
  cr: RotationCreative,
  objective: ObjectiveType,
  benches: CreativeBenches
): FunnelStage[] {
  const video = cr.type === "video";
  const conv = objective !== "awareness";
  const stages: FunnelStage[] = [];

  if (video) {
    stages.push({
      label: "HOOK RATE",
      value: cr.hook_rate,
      bench: benches.hook_rate,
      format: formatRate,
    });
  } else {
    stages.push({
      label: "HOOK RATE",
      value: null,
      na: "n/a (static)",
      format: formatRate,
    });
  }

  if (video) {
    stages.push({
      label: "WATCHED",
      value: cr.completion_rate,
      bench: benches.completion_rate,
      format: formatRate,
      primary: !conv,
    });
  } else {
    stages.push({
      label: "ENGAGED",
      value: cr.engagement_rate,
      bench: benches.engagement_rate,
      format: formatRate,
      primary: !conv,
    });
  }

  if (conv) {
    stages.push({
      label: "CLICKED",
      value: cr.ctr,
      bench: benches.ctr,
      format: formatRate,
    });
    stages.push({
      label: "CONVERTED",
      value: cr.cpa,
      bench: benches.cpa,
      format: formatMoney,
      primary: true,
      na: "NO RESULTS YET",
    });
  } else {
    if (video) {
      stages.push({
        label: "ENGAGED",
        value: cr.engagement_rate,
        bench: benches.engagement_rate,
        format: formatRate,
      });
    } else {
      stages.push({
        label: "FREQUENCY",
        value: cr.frequency,
        bench: benches.frequency,
        format: formatTimes,
      });
    }
    stages.push({
      label: "CLICKED",
      value: cr.ctr,
      bench: benches.ctr,
      format: formatRate,
      notKpi: true,
    });
  }
  return stages;
}

function StageRow({ s }: { s: FunnelStage }) {
  return (
    <div className="grid grid-cols-[78px_62px_1fr] items-center gap-2">
      <div>
        <div
          className={cn(
            "font-mono text-[9px] tracking-[0.07em]",
            s.primary ? "font-bold text-accent-ink" : "text-fg-faint"
          )}
        >
          {s.label}
        </div>
        {s.primary && (
          <div className="mt-px">
            <Glossary
              termKey="sets_rank"
              variant="icon"
              className="font-mono text-[7px] tracking-[0.08em] text-accent-ink"
            >
              ✱ SETS RANK
            </Glossary>
          </div>
        )}
        {s.notKpi && (
          <div className="mt-px font-mono text-[7px] tracking-[0.08em] text-fg-faint">
            NOT A KPI
          </div>
        )}
      </div>
      <span
        className={cn(
          "tnum font-mono text-[12.5px] font-bold",
          s.value == null ? "text-fg-faint" : "text-fg"
        )}
      >
        {s.value == null ? "—" : s.format(s.value)}
      </span>
      {s.value == null ? (
        <span className="font-mono text-[8.5px] text-fg-faint">
          {s.na ?? "NOT REPORTED"}
        </span>
      ) : (
        <QuartileBar value={s.value} bench={s.bench ?? null} width={120} />
      )}
    </div>
  );
}

/* ── #11: per-platform video drop-off — once watching, how far ───── */

/**
 * Retention read for a video creative, per platform. A lead-in carries the
 * pre-start loss — hook (3-second views) and the share of impressions still
 * there at the 25% mark, both as shares of impressions, so the early
 * scroll-away is in the same block. The four bars below then re-anchor at each
 * platform's video START (the 3-second intentional view, q25 fallback) so the
 * start → 25 → 50 → 75 → complete funnel reads honestly and platforms compare
 * fairly regardless of how many people started the video on each (ADA
 * 1215989989043460). Renders nothing when no platform cell carries a start.
 */
function VideoDropoff({
  cells,
}: {
  /** matrixCells[cr.variant] — the current creative's per-platform row. */
  cells: Record<string, CreativeMatrixCell> | undefined;
}) {
  if (!cells) return null;
  const rows = Object.entries(cells).filter(
    ([, cell]) => cell.video_start > 0
  );
  if (rows.length === 0) return null;

  const marks: Array<{ label: string; get: (c: CreativeMatrixCell) => number }> =
    [
      { label: "25%", get: (c) => c.video_q25 },
      { label: "50%", get: (c) => c.video_q50 },
      { label: "75%", get: (c) => c.video_q75 },
      { label: "100%", get: (c) => c.video_q100 },
    ];

  return (
    <div className="mx-4 mt-3 rounded-sm bg-surface-sunken px-3 py-2.5">
      <div className="font-mono text-[8px] tracking-[0.1em] text-fg-faint">
        THE DROP-OFF · HOOK TO FINISH
      </div>
      <div className="mt-2 flex flex-col gap-3">
        {rows.map(([platformId, cell]) => {
          const base = cell.video_start;
          const finished = Math.min(100, Math.round((cell.video_q100 / base) * 100));
          const hook = cell.hook_rate;
          // Share of impressions still there at the 25% mark. Gated on the same
          // 1,000-impression floor the backend nulls the sibling rates at, so a
          // thin cell never shows an extreme reach beside an honestly-nulled
          // hook. formatRate renders "—" when either read is held back.
          const reach25 =
            cell.impressions >= VOLUME_MIN_IMPRESSIONS
              ? cell.video_q25 / cell.impressions
              : null;
          return (
            <div key={platformId}>
              <div className="flex items-baseline justify-between gap-2">
                <span className="font-mono text-[9px] font-bold tracking-[0.08em] text-fg-secondary">
                  {platformLabel(platformId)}
                </span>
                {/* Pre-25% lead-in: two independent shares of impressions with a
                    neutral separator. Hook (3s views) and the 25% mark come from
                    different platform events and need not be monotonic, so this
                    reads as two numbers — never a subtracted "drop". */}
                <span className="font-mono text-[8.5px] text-fg-faint">
                  <span className="text-fg-secondary">Hook {formatRate(hook)}</span>{" "}
                  ·{" "}
                  <span className="text-fg-secondary">
                    at 25% {formatRate(reach25)}
                  </span>{" "}
                  of impressions
                </span>
              </div>
              <div className="mt-1.5 font-mono text-[7.5px] tracking-[0.06em] text-fg-faint">
                Once started · of the {formatNumberCompact(base)} who started the video
              </div>
              <div className="mt-1.5 grid grid-cols-4 gap-1.5">
                {marks.map((m) => {
                  const retention = Math.min(100, Math.round((m.get(cell) / base) * 100));
                  return (
                    <div key={m.label} className="flex flex-col items-center gap-1">
                      <div className="flex h-[26px] w-full items-end justify-center">
                        <div
                          className="w-full rounded-xs bg-accent"
                          style={{
                            height: `${Math.max(2, retention)}%`,
                            opacity: 0.28 + (retention / 100) * 0.55,
                          }}
                        />
                      </div>
                      <div className="tnum font-mono text-[9px] font-bold text-fg">
                        {retention}%
                      </div>
                      <div className="font-mono text-[7.5px] tracking-[0.06em] text-fg-faint">
                        {m.label}
                      </div>
                    </div>
                  );
                })}
              </div>
              <div className="mt-1 font-mono text-[8.5px] text-fg-muted">
                Finished: {finished}% of those who started
              </div>
            </div>
          );
        })}
      </div>
      <div className="mt-2.5 font-mono text-[8px] leading-[1.5] text-fg-faint">
        Lead-in is share of impressions — hooked at 3 seconds, then still there
        at the 25% mark. The bars below re-anchor at the video start and show
        how many of those who started were still watching at each quarter.
        Shown per platform.
      </div>
      <div className="mt-0.5 font-mono text-[8px] leading-[1.5] text-fg-faint opacity-70">
        The bars re-anchor at the video start on each platform, so they compare
        fairly regardless of how many people started.
      </div>
    </div>
  );
}

/* ── Best audience for a creative — the Audiences-page echo ──────── */

/**
 * Retargeting pools are excluded unless nothing else qualifies: warm
 * audiences are always cheapest, so "best" there is a false signal for
 * the next dollar.
 */
function bestAudienceFor(
  variant: string,
  objective: ObjectiveType,
  audMatrix: AudienceMatrixResponse | null
): MatrixAudience | null {
  if (!audMatrix) return null;
  const pick = (skipRetargeting: boolean) => {
    let best: { score: number; audience: MatrixAudience } | null = null;
    for (const audience of audMatrix.audiences) {
      if (
        skipRetargeting &&
        (audience.role ?? "").toLowerCase() === "retargeting"
      ) {
        continue;
      }
      const cell = audMatrix.cells[audience.id]?.[variant];
      if (!cell) continue;
      const score =
        objective === "awareness"
          ? cell.completion_rate ?? (cell.ctr != null ? cell.ctr * 10 : null)
          : cell.cpa != null
            ? -cell.cpa
            : null;
      if (score == null) continue;
      if (!best || score > best.score) best = { score, audience };
    }
    return best;
  };
  return (pick(true) ?? pick(false))?.audience ?? null;
}

/* ── One creative card — the attention funnel is the spine ───────── */

function RotationCard({
  j,
  objective,
  benches,
  coverage,
  bestAud,
  matrixCells,
  onTab,
  onRename,
}: {
  j: JudgedCreative;
  objective: ObjectiveType;
  benches: CreativeBenches;
  coverage: RotationCoverage | undefined;
  bestAud: MatrixAudience | null;
  /** #11: this creative's per-platform matrix row (matrix.cells[variant]),
   *  for the video drop-off read. */
  matrixCells: Record<string, CreativeMatrixCell> | undefined;
  onTab: (tab: string) => void;
  onRename: (oldVariant: string, value: string) => void;
}) {
  const cr = j.creative;
  const stages = buildStages(cr, objective, benches);
  const ctrTrend = cr.trend?.ctr ?? [];
  const freqTrend = cr.trend?.frequency ?? [];
  const ctrFalling =
    ctrTrend.length >= 2 &&
    ctrTrend[ctrTrend.length - 1] < ctrTrend[0] * 0.92;
  const latestFreq = freqTrend.length
    ? freqTrend[freqTrend.length - 1]
    : cr.frequency;
  const freqHot = latestFreq != null && latestFreq > 4;
  const tinted = j.verdict === "SCALE" || j.verdict === "REFRESH";

  // Honesty line: which of this creative's platforms actually report
  // its attention (video) or engagement (static) numbers.
  const covList =
    cr.type === "video" ? coverage?.completion ?? [] : coverage?.engagement ?? [];
  const reports = cr.platforms.filter((p) => covList.includes(p));
  const missing =
    covList.length > 0 ? cr.platforms.filter((p) => !covList.includes(p)) : [];

  /* #19: an unresolved name gets the NEEDS A NAME chip and a reason built
     here, where spend + impressions are formatted. Spend/impressions stay
     visible and the card stays in the ranked list. */
  const unresolved = isUnresolvedVariant(cr.variant);
  const dimensionsOnly = /^\s*\d+\s*[x×]\s*\d+\s*$/.test(cr.variant);
  const unresolvedReason = dimensionsOnly
    ? `Its name came through as raw dimensions (${cr.variant}), so it has not been matched to an asset and is not graded. It has spent ${formatCurrencyCompact(
        cr.spend
      )} across ${formatNumberCompact(
        cr.impressions
      )} impressions. Give it a name to fold it into the rotation read.`
    : `This creative has not been matched to a name yet, so it is not graded. It has spent ${formatCurrencyCompact(
        cr.spend
      )} across ${formatNumberCompact(
        cr.impressions
      )} impressions. Give it a name to fold it into the rotation read.`;

  return (
    <Card
      className="flex flex-col overflow-hidden p-0"
      style={
        tinted
          ? { borderColor: `color-mix(in srgb, ${j.tone} 45%, var(--border))` }
          : undefined
      }
    >
      <div className="px-4 pt-3.5">
        {/* The still from the asset sync, when stored; the abstract
            glyph frame otherwise. The VIDEO/STATIC chip stays overlaid
            either way. */}
        <PlacementFrame
          type={cr.type}
          platforms={cr.platforms}
          alt={cr.variant}
          imageUrl={
            /* The proxy returns an API-relative path; absolutize it. */
            cr.image_url && cr.image_url.startsWith("/")
              ? `${API_BASE}${cr.image_url}`
              : cr.image_url
          }
        />
        <div className="mt-3 flex items-center justify-between gap-2.5">
          <AliasName
            name={cr.variant}
            onRename={(v) => onRename(cr.variant, v)}
            showNameCta={unresolved}
          />
          <CreativeVerdictChip verdict={j.verdict} />
        </div>
        <div className="mt-2 flex items-center gap-2">
          {cr.platforms.map((p) => (
            <PlatformIcon key={p} platformId={p} size={20} />
          ))}
          <span className="font-mono text-[10px] text-fg-muted">
            {formatCurrencyCompact(cr.spend)} ·{" "}
            {Math.round(cr.spend_share * 100)}% of rotation
          </span>
        </div>
        <p className="mt-2.5 min-h-[40px] text-xs leading-[1.55] text-fg-secondary">
          {unresolved ? unresolvedReason : j.reason}
        </p>
      </div>

      <div className="flex flex-col gap-2 px-4 pt-3">
        {stages.map((s) => (
          <StageRow key={s.label} s={s} />
        ))}
      </div>

      {cr.type === "video" && <VideoDropoff cells={matrixCells} />}

      <div className="mx-4 mt-3 flex items-center gap-3.5 rounded-sm bg-surface-sunken px-3 py-2">
        <div>
          <div className="font-mono text-[8px] tracking-[0.1em] text-fg-faint">
            CTR · LAST 8 DAYS
          </div>
          <Spark
            data={ctrTrend}
            width={74}
            height={17}
            color={ctrFalling ? "var(--danger)" : "var(--ok)"}
          />
        </div>
        <div>
          <div className="font-mono text-[8px] tracking-[0.1em] text-fg-faint">
            FREQUENCY · LAST 8 DAYS
          </div>
          <Spark
            data={freqTrend}
            width={74}
            height={17}
            color={freqHot ? "var(--danger)" : "var(--text-muted)"}
          />
        </div>
        <span
          className={cn(
            "ml-auto font-mono text-[9.5px] font-semibold",
            freqHot ? "text-danger" : "text-fg-muted"
          )}
        >
          {latestFreq != null ? formatTimes(latestFreq) : "—"}
        </span>
      </div>

      <div className="mt-auto flex flex-col gap-1 px-4 pb-3.5 pt-2.5">
        {bestAud && (
          <button
            onClick={() => onTab("audiences")}
            className="text-left font-mono text-[9.5px] text-fg-muted transition-colors hover:text-fg"
          >
            Lands best with{" "}
            <span className="font-bold text-accent-ink">{bestAud.name} →</span>
          </button>
        )}
        <CoverageLine
          label={cr.type === "video" ? "Attention metrics" : "Engagement"}
          reports={reports}
          missing={missing}
        />
      </div>
    </Card>
  );
}

/* ── Creative × platform — is the winner winning everywhere? ─────── */

function rowSpend(row: Record<string, CreativeMatrixCell> | undefined): number {
  if (!row) return 0;
  return Object.values(row).reduce((sum, c) => sum + (c.spend ?? 0), 0);
}

function PlatformMatrix({
  matrix,
  judged,
  objective,
  benches,
  perf,
}: {
  matrix: CreativeMatrixResponse;
  judged: JudgedCreative[];
  objective: ObjectiveType;
  benches: CreativeBenches;
  perf: PerformanceResponse | null;
}) {
  // The CPM lens lives here and only here: rooms have prices.
  const lenses = lensesFor(objective);
  const [lensRaw, setLens] = useState<LensId>(lenses[0].id);
  const lens = lenses.some((l) => l.id === lensRaw) ? lensRaw : lenses[0].id;
  const activeLens = lenses.find((l) => l.id === lens)!;

  const byVariant = useMemo(
    () => new Map(judged.map((x) => [x.creative.variant, x])),
    [judged]
  );
  const plats = matrix.platforms;
  const cols = `minmax(0,1.15fr) repeat(${plats.length}, minmax(108px, 1fr))`;
  const clickDefs = perf?.clicks_definitions
    ? Object.values(perf.clicks_definitions)
    : [];

  // Order rows by rotation rank where we can; unknown variants last.
  const rowOrder = [...matrix.creatives].sort((a, b) => {
    const ia = judged.findIndex((x) => x.creative.variant === a);
    const ib = judged.findIndex((x) => x.creative.variant === b);
    return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
  });

  return (
    <Card className="overflow-hidden p-0">
      <div className="flex flex-wrap items-center gap-3 border-b border-line-soft bg-surface-sunken px-4 py-3">
        <span className="font-mono text-[8.5px] tracking-[0.12em] text-fg-faint">
          CELLS SHOW
        </span>
        <LensSwitch lenses={lenses} lens={lens} onLens={setLens} />
        <span className="flex-[1_1_260px] text-right text-[11px] text-fg-muted">
          {activeLens.explain}
        </span>
      </div>
      <div className="overflow-x-auto">
        <div className="min-w-[640px]">
          <div
            className="grid border-b-2 border-line"
            style={{ gridTemplateColumns: cols }}
          >
            <div className="flex items-end px-4 py-2.5">
              <span className="font-mono text-[8.5px] tracking-[0.12em] text-fg-faint">
                CREATIVE ↓ · PLATFORM →
              </span>
            </div>
            {plats.map((p) => (
              <div
                key={p.platform_id}
                className="border-l border-line-soft px-2 py-2.5 text-center"
              >
                <div className="flex items-center justify-center gap-1.5">
                  <PlatformIcon platformId={p.platform_id} size={18} />
                  <span className="text-[11.5px] font-bold text-fg">
                    {platformLabel(p.platform_id)}
                  </span>
                </div>
                <div className="mt-[3px] font-mono text-[8px] text-fg-faint">
                  {formatCurrencyCompact(p.spend)} · {Math.round(p.share * 100)}%
                  of spend
                </div>
              </div>
            ))}
          </div>
          {rowOrder.map((variant, i) => {
            const row = matrix.cells[variant];
            const rowJudged = byVariant.get(variant);
            const isStatic = rowJudged?.creative.type === "static";
            return (
              <div
                key={variant}
                className={cn(
                  "grid items-stretch",
                  i > 0 && "border-t border-line-soft"
                )}
                style={{ gridTemplateColumns: cols }}
              >
                <div className="min-w-0 px-4 py-2.5">
                  <div className="flex min-w-0 items-center gap-2">
                    <span className="truncate text-[12.5px] font-extrabold text-fg">
                      {variant}
                    </span>
                    {rowJudged && (
                      <CreativeVerdictChip verdict={rowJudged.verdict} size="sm" />
                    )}
                  </div>
                  <div className="mt-[3px] font-mono text-[8.5px] text-fg-faint">
                    {rowJudged
                      ? (rowJudged.creative.type === "video" ? "VIDEO" : "STATIC") +
                        " · "
                      : ""}
                    {formatCurrencyCompact(
                      rowJudged?.creative.spend ?? rowSpend(row)
                    )}{" "}
                    total
                  </div>
                </div>
                {plats.map((p) => {
                  const cell = row?.[p.platform_id] ?? null;
                  return (
                    <div
                      key={p.platform_id}
                      className="flex flex-col justify-center border-l border-line-soft"
                    >
                      <MatrixCell
                        cell={cell}
                        lens={lens}
                        benches={benches}
                        emptyLabel="NOT RUNNING"
                        naTag={
                          isStatic && (lens === "hook" || lens === "completion")
                            ? "STATIC · N/A"
                            : undefined
                        }
                        spend={cell?.spend ?? null}
                      />
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>
      </div>
      <div className="border-t-2 border-line bg-surface-sunken px-4 py-2.5 font-mono text-[9px] leading-[1.7] text-fg-faint">
        &ldquo;NOT RUNNING&rdquo; can be an opportunity, not just a gap.
        {clickDefs.length > 0 && (
          <> What counts as a click varies: {clickDefs.join(" · ")}.</>
        )}
      </div>
    </Card>
  );
}

/* ── Reporting strip — defensible numbers, caveats attached ──────── */

function ReportTile({
  label,
  value,
  verdict,
  sub,
}: {
  /** #5: widened to ReactNode so a label can carry a Glossary wrapper. */
  label: React.ReactNode;
  value: string;
  verdict?: React.ReactNode;
  sub?: string;
}) {
  return (
    <div className="min-w-0 border-l border-line-soft px-3.5 py-3 first:border-l-0">
      <div className="font-mono text-[8.5px] uppercase tracking-[0.1em] text-fg-faint">
        {label}
      </div>
      <div className="tnum mt-1.5 font-display text-[25px] uppercase leading-none text-fg">
        {value}
      </div>
      <div className="mt-[5px] min-h-[12px]">{verdict}</div>
      {sub && (
        <div className="mt-1 font-mono text-[9px] leading-[1.5] text-fg-faint">
          {sub}
        </div>
      )}
    </div>
  );
}

const SCALE_METRIC_NOTE = (
  <span
    className="font-mono text-[8.5px] tracking-[0.1em] text-fg-faint"
    title="Scale metrics measure size, not quality: no benchmark applies"
  >
    SCALE METRIC
  </span>
);

/* #5: the report-ready flag — platform-attributed conversions are the
   single defensible figure for the client report. */
const REPORT_THIS_NOTE = (
  <span
    className="font-mono text-[8.5px] tracking-[0.1em] text-accent-ink"
    title="Platform-attributed conversions: the single defensible number for the client report. GA4 tracks a broader set of site key events separately."
  >
    REPORT THIS
  </span>
);

/* ── After the click — the GA4 funnel ────────────────────────────── */

function FunnelBox({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="rounded-sm bg-surface-sunken px-3.5 py-3">
      <div className="font-mono text-[8.5px] uppercase tracking-[0.1em] text-fg-faint">
        {label}
      </div>
      <div className="tnum mt-1.5 font-display text-[23px] uppercase leading-none text-fg">
        {value}
      </div>
      {sub && (
        <div className="mt-1 font-mono text-[9px] leading-[1.45] text-fg-faint">
          {sub}
        </div>
      )}
    </div>
  );
}

function AfterTheClick({
  ga4,
  perf,
  onTab,
}: {
  ga4: GA4PerformanceResponse | null;
  perf: PerformanceResponse | null;
  onTab: (tab: string) => void;
}) {
  if (!ga4 || !ga4.has_ga4) {
    return (
      <Card className="flex flex-wrap items-center gap-3">
        <Label className="text-fg-secondary">After the click</Label>
        <span className="text-[12.5px] text-fg-muted">
          GA4 is not connected on this campaign: paid metrics stop at the
          click.
        </span>
        <span className="ml-auto">
          <Btn
            variant="outline"
            size="sm"
            icon={<Plug className="h-3.5 w-3.5" />}
            onClick={() => onTab("settings")}
          >
            Connect GA4
          </Btn>
        </span>
      </Card>
    );
  }

  const clicks = perf?.total_clicks ?? null;
  const sessions = ga4.total_sessions;
  const conversions = ga4.total_conversions;
  const arrival =
    clicks != null && clicks > 0 ? (sessions / clicks) * 100 : null;
  const sessionRate = sessions > 0 ? (conversions / sessions) * 100 : null;
  const bounce =
    ga4.avg_bounce_rate != null ? ga4.avg_bounce_rate * 100 : null;
  const avgSession =
    ga4.avg_session_duration != null
      ? `${Math.round(ga4.avg_session_duration)}s`
      : null;
  const arrivalTone =
    arrival == null
      ? "var(--text-muted)"
      : arrival >= 80
        ? "var(--ok)"
        : arrival >= 60
          ? "var(--warn)"
          : "var(--danger)";
  const leak =
    arrival != null && arrival < 90
      ? `${Math.max(0, Math.round(100 - arrival))}% of paid clicks never produce a session. The gap usually lives in page load and redirects, not in the media.`
      : null;
  const platformConv = perf?.total_conversions;

  return (
    <Card>
      <SectionHead right="GA4 · same twice-daily sync">
        After the click: does the site keep what media buys?
      </SectionHead>
      <div className="grid items-center gap-2 sm:grid-cols-[1fr_auto_1fr_auto_1fr]">
        <FunnelBox
          label="Paid clicks"
          value={clicks != null ? formatNumberCompact(clicks) : "—"}
          sub="Click definitions vary by platform"
        />
        <div className="px-3 text-center">
          <div
            className="font-mono text-[10.5px] font-bold"
            style={{ color: arrivalTone }}
          >
            →{" "}
            {arrival != null
              ? `${arrival.toFixed(0)}% arrive${
                  arrival >= 100 ? " (over 100% is normal)" : ""
                }`
              : "—"}
          </div>
        </div>
        <FunnelBox
          label="Sessions (GA4)"
          value={formatNumberCompact(sessions)}
          sub={
            [
              bounce != null ? `Bounce ${bounce.toFixed(0)}%` : null,
              avgSession ? `avg ${avgSession}` : null,
            ]
              .filter(Boolean)
              .join(" · ") || undefined
          }
        />
        <div className="px-3 text-center">
          <div className="font-mono text-[10.5px] font-bold text-fg-muted">
            → {sessionRate != null ? `${sessionRate.toFixed(1)}% of sessions` : "—"}
          </div>
        </div>
        <FunnelBox
          label="Conversions (GA4)"
          value={formatNumberCompact(conversions)}
          sub={
            platformConv != null && platformConv > 0
              ? `The platform figure (${formatNumberCompact(
                  Math.round(platformConv)
                )}) is the one for the report. GA4 counts a wider set of site key events, so the two will not match.`
              : "GA4 counts a wider set of site key events than the platforms report, so these two will not match."
          }
        />
      </div>
      {leak && (
        <p className="mt-3 text-[11.5px] leading-relaxed text-fg-muted">
          <b className="text-warn">The leak is between click and session: </b>
          {leak}
        </p>
      )}
    </Card>
  );
}

/* ── The long tables — reference material, collapsed by default ──── */

function LongTablesDrawer({
  code,
  objective,
}: {
  code: string;
  objective: ObjectiveType;
}) {
  const [open, setOpen] = useState(false);
  const [ads, setAds] = useState<AdPerformanceResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const conv = objective !== "awareness";

  const toggle = () => {
    const next = !open;
    setOpen(next);
    if (next && !ads && !loading) {
      setLoading(true);
      api.performance
        .ads(code)
        .then(setAds)
        .catch(() => setAds(null))
        .finally(() => setLoading(false));
    }
  };

  return (
    <Card className="overflow-hidden p-0">
      <button
        onClick={toggle}
        className="flex w-full flex-wrap items-center gap-3 px-[18px] py-3 text-left"
      >
        <span className="font-mono text-[11px] font-bold tracking-[0.1em] text-fg-secondary">
          {open ? "▾" : "▸"} THE LONG TABLES
        </span>
        <span className="font-mono text-[10px] text-fg-faint">
          {ads ? `Ads (${ads.ads.length}) · ` : ""}per-platform detail
        </span>
        <span className="ml-auto font-mono text-[9.5px] text-fg-faint">
          reference material: it doesn&apos;t set the tone of the page
        </span>
      </button>
      {open && (
        <div className="border-t border-line-soft px-[18px] py-3.5">
          {loading && <p className="py-3 text-xs text-fg-muted">Loading ads…</p>}
          {!loading && (!ads || ads.ads.length === 0) && (
            <p className="py-3 text-xs text-fg-muted">
              No ad-level rows available.
            </p>
          )}
          {!loading && ads && ads.ads.length > 0 && (
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-left text-[11.5px]">
                <thead>
                  <tr>
                    {(
                      [
                        "Ad",
                        "Ad set",
                        "Platform",
                        "Spend",
                        "Impr.",
                        "CTR",
                        "Outbound clicks",
                        "Landing page views",
                        conv ? "Conv." : "VCR",
                        conv ? "CPA" : null,
                      ].filter(Boolean) as string[]
                    ).map((h, i) => (
                      <th
                        key={h}
                        className={cn(
                          "pb-2 pr-2.5 font-mono text-[8.5px] font-medium uppercase tracking-[0.1em] text-fg-faint",
                          i >= 3 && "text-right"
                        )}
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {ads.ads.map((ad, i) => (
                    <tr
                      key={ad.ad_id ?? `${ad.ad_name ?? "ad"}-${i}`}
                      className="border-t border-line-soft"
                    >
                      <td className="max-w-[260px] truncate py-[7px] pr-2.5 font-mono text-[10.5px] text-fg">
                        {ad.ad_name ?? "—"}
                      </td>
                      <td className="max-w-[200px] truncate py-[7px] pr-2.5 text-fg-secondary">
                        {ad.ad_set_name ?? "—"}
                      </td>
                      <td className="py-[7px] pr-2.5">
                        <span className="inline-flex items-center gap-1.5">
                          <PlatformIcon platformId={ad.platform_id} size={18} />
                          <span className="text-[11px] text-fg-secondary">
                            {platformLabel(ad.platform_id)}
                          </span>
                        </span>
                      </td>
                      <td className="tnum py-[7px] pr-2.5 text-right font-mono font-semibold text-fg">
                        {formatCurrencyCompact(ad.spend)}
                      </td>
                      <td className="tnum py-[7px] pr-2.5 text-right font-mono text-fg-secondary">
                        {formatNumberCompact(ad.impressions)}
                      </td>
                      <td className="tnum py-[7px] pr-2.5 text-right font-mono text-fg-secondary">
                        {ad.ctr != null ? formatRate(ad.ctr) : "—"}
                      </td>
                      <td className="tnum py-[7px] pr-2.5 text-right font-mono text-fg-secondary">
                        {ad.outbound_clicks != null
                          ? formatNumberCompact(ad.outbound_clicks)
                          : "—"}
                      </td>
                      <td className="tnum py-[7px] pr-2.5 text-right font-mono text-fg-secondary">
                        {ad.landing_page_views != null
                          ? formatNumberCompact(ad.landing_page_views)
                          : "—"}
                      </td>
                      {conv ? (
                        <>
                          <td className="tnum py-[7px] pr-2.5 text-right font-mono text-fg-secondary">
                            {ad.conversions > 0
                              ? formatNumber(Math.round(ad.conversions))
                              : "—"}
                          </td>
                          <td className="tnum py-[7px] text-right font-mono text-fg-secondary">
                            {ad.conversions > 0
                              ? formatMoney(ad.spend / ad.conversions)
                              : "—"}
                          </td>
                        </>
                      ) : (
                        <td className="tnum py-[7px] text-right font-mono text-fg-secondary">
                          {ad.vcr != null ? formatRate(ad.vcr) : "—"}
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

/* ── The tab ─────────────────────────────────────────────────────── */

export function CreativeTab({
  code,
  onTab,
}: {
  code: string;
  onTab: (tab: string) => void;
}) {
  const [rotation, setRotation] = useState<CreativeRotationResponse | null>(
    null
  );
  const [matrix, setMatrix] = useState<CreativeMatrixResponse | null>(null);
  const [bench, setBench] = useState<BenchmarkResponse | null>(null);
  const [perf, setPerf] = useState<PerformanceResponse | null>(null);
  const [ga4, setGa4] = useState<GA4PerformanceResponse | null>(null);
  const [audMatrix, setAudMatrix] = useState<AudienceMatrixResponse | null>(
    null
  );
  const [variantRows, setVariantRows] =
    useState<CreativeVariantResponse | null>(null);
  const [win, setWin] = useState<RotationWindow>("flight");
  const [loading, setLoading] = useState(true);
  const [windowLoading, setWindowLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    setWin("flight");
    Promise.all([
      api.creative.rotation(code, "flight").catch(() => null),
      api.creative.matrix(code).catch(() => null),
      api.benchmarks.get(code).catch(() => null),
      api.performance.get(code).catch(() => null),
      api.ga4.analytics(code).catch(() => null),
      api.audiences.matrix(code).catch(() => null),
      // The rotation payload doesn't carry raw ad names; the alias-rename
      // endpoint needs them as patterns, so we fetch the variant rows too.
      api.performance.creatives(code).catch(() => null),
    ])
      .then(([rot, mat, b, p, g, aud, variants]) => {
        setRotation(rot);
        setMatrix(mat);
        setBench(b);
        setPerf(p);
        setGa4(g);
        setAudMatrix(aud);
        setVariantRows(variants);
      })
      .finally(() => setLoading(false));
  }, [code]);

  const changeWindow = (w: RotationWindow) => {
    if (w === win || windowLoading) return;
    setWin(w);
    setWindowLoading(true);
    api.creative
      .rotation(code, w)
      .then(setRotation)
      .catch(() => undefined)
      .finally(() => setWindowLoading(false));
  };

  /* Optimistic alias rename — mirror the new name into every payload
     that keys on the variant, without refetching. */
  const renameVariant = (oldV: string, newV: string) => {
    const renameKeys = <T,>(rec: Record<string, T>): Record<string, T> => {
      if (!(oldV in rec)) return rec;
      const next: Record<string, T> = {};
      for (const [k, v] of Object.entries(rec)) {
        next[k === oldV ? newV : k] = v;
      }
      return next;
    };
    setRotation(
      (prev) =>
        prev && {
          ...prev,
          creatives: prev.creatives.map((c) =>
            c.variant === oldV ? { ...c, variant: newV } : c
          ),
        }
    );
    setMatrix(
      (prev) =>
        prev && {
          ...prev,
          creatives: prev.creatives.map((v) => (v === oldV ? newV : v)),
          cells: renameKeys(prev.cells),
        }
    );
    setAudMatrix(
      (prev) =>
        prev && {
          ...prev,
          creatives: prev.creatives.map((v) => (v === oldV ? newV : v)),
          cells: Object.fromEntries(
            Object.entries(prev.cells).map(([aid, row]) => [
              aid,
              renameKeys(row),
            ])
          ),
        }
    );
    setVariantRows(
      (prev) =>
        prev && {
          ...prev,
          creatives: prev.creatives.map((r) =>
            r.creative_variant === oldV
              ? { ...r, creative_variant: newV }
              : r
          ),
        }
    );
  };

  const handleRename = async (oldV: string, raw: string) => {
    const next = raw.trim();
    if (!next || next === oldV) return;
    const adNames =
      variantRows?.creatives.find((r) => r.creative_variant === oldV)
        ?.ad_names ?? [oldV];
    try {
      for (const adName of adNames) {
        await api.admin.createCreativeAlias({
          project_code: code,
          ad_name_pattern: adName,
          creative_variant: next,
        });
      }
      renameVariant(oldV, next);
    } catch {
      /* keep the old name; the backend rejected the alias */
    }
  };

  const objective: ObjectiveType =
    rotation?.objective ?? perf?.objective_type ?? "mixed";
  const benches = useMemo(() => buildBenches(bench), [bench]);
  const judged = useMemo(
    () =>
      rotation
        ? rankCreatives(
            rotation.creatives.map((c) => judgeCreative(c, objective, benches)),
            objective
          )
        : [],
    [rotation, objective, benches]
  );
  const pk = primaryKpi(objective);
  const call = useMemo(
    () => buildCreativeCall(judged, objective),
    [judged, objective]
  );
  const imbalance = useMemo(
    () => rotationImbalance(judged, objective, pk.label),
    [judged, objective, pk.label]
  );

  if (loading) {
    return (
      <div className="space-y-4">
        <p className="text-xs text-fg-muted">
          Loading creative, the ad previews take a little while to come through…
        </p>
        <Card className="animate-pulse">
          <div className="h-9 w-72 rounded bg-surface-sunken" />
          <div className="mt-4 h-4 w-96 rounded bg-surface-sunken" />
        </Card>
        <div className="grid gap-3 md:grid-cols-3">
          {[0, 1, 2].map((i) => (
            <Card key={i} className="h-64 animate-pulse" />
          ))}
        </div>
      </div>
    );
  }

  if (!rotation || rotation.creatives.length === 0) {
    return <AdLevelAwaiting />;
  }

  const totals = rotation.totals;
  const cov = rotation.coverage;
  const covNames = (ids: string[]) => ids.map(platformLabel).join(" + ");

  /* #5: which conversions number the report should quote. Rotation totals
     carry the platform-attributed figure; GA4 is the broader site-events
     count. When the platform number is present, it is the one to report. */
  const platformConv = totals.conversions ?? perf?.total_conversions ?? 0;
  const ga4Conv = ga4?.total_conversions ?? 0;
  const reportConv = pickReportConversions(platformConv, ga4Conv, objective);
  /* #5: the report-totals arrival read, so we can de-alarm ">100% arrive". */
  const reportArrival =
    perf?.total_clicks && perf.total_clicks > 0
      ? (ga4?.total_sessions ?? 0) / perf.total_clicks * 100
      : null;

  return (
    <div className="flex flex-col gap-4">
      {/* the call */}
      <Card className="p-6 sm:p-7">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <Eyebrow>The call · creative rotation</Eyebrow>
          <span className="inline-flex items-center gap-1.5 whitespace-nowrap font-mono text-[10.5px] text-fg-faint">
            Data as of {rotation.as_of} ·
            <SyncStatus variant="compact" />
          </span>
        </div>
        <div className="display mt-3 text-[34px] text-fg sm:text-[40px]">
          {call.headline}
        </div>
        <p className="mt-2.5 max-w-[720px] text-sm leading-[1.55] text-fg-secondary">
          {call.body}
        </p>
        <div className="mt-4 flex flex-wrap items-baseline gap-3 border-t border-line-soft pt-3">
          <span className="whitespace-nowrap font-mono text-[9.5px] font-bold tracking-[0.1em] text-accent-ink">
            ✱ RANKED BY {pk.label.toUpperCase()}
          </span>
          <span className="max-w-[760px] text-xs leading-[1.5] text-fg-muted">
            {pk.why}
          </span>
        </div>
      </Card>

      {/* warnings, straight from the performance endpoint */}
      {(perf?.high_frequency_warning ||
        (objective !== "awareness" && perf?.zero_conversion_warning)) && (
        <div className="flex flex-col gap-1.5">
          {perf?.high_frequency_warning && (
            <WarnStrip kind="frequency">{perf.high_frequency_warning}</WarnStrip>
          )}
          {objective !== "awareness" && perf?.zero_conversion_warning && (
            <WarnStrip kind="tracking">{perf.zero_conversion_warning}</WarnStrip>
          )}
        </div>
      )}

      {/* the rotation */}
      <div>
        <SectionHead
          right={
            <span className="inline-flex flex-wrap items-center gap-2">
              <span>
                {judged.length} creative{judged.length === 1 ? "" : "s"} ·
                ranked by {pk.label.toLowerCase()}
              </span>
              <span className="inline-flex gap-1">
                {(["flight", "7d"] as RotationWindow[]).map((w) => (
                  <button
                    key={w}
                    onClick={() => changeWindow(w)}
                    className={cn(
                      "rounded-xs border px-2 py-0.5 font-mono text-[8.5px] font-bold tracking-[0.08em] transition-colors duration-fast",
                      win === w
                        ? "border-accent-ink bg-tint-accent text-accent-ink"
                        : "border-line text-fg-muted hover:text-fg"
                    )}
                  >
                    {w === "flight" ? "FLIGHT" : "LAST 7D"}
                  </button>
                ))}
              </span>
            </span>
          }
        >
          The rotation: who earns the next dollar?
        </SectionHead>
        <div
          className={cn(
            "grid grid-cols-[repeat(auto-fit,minmax(300px,1fr))] items-stretch gap-3",
            windowLoading && "opacity-60"
          )}
        >
          {judged.map((j) => (
            <RotationCard
              key={j.creative.variant}
              j={j}
              objective={objective}
              benches={benches}
              coverage={cov}
              bestAud={bestAudienceFor(j.creative.variant, objective, audMatrix)}
              matrixCells={matrix?.cells[j.creative.variant]}
              onTab={onTab}
              onRename={handleRename}
            />
          ))}
        </div>
        <div className="mt-2 font-mono text-[9px] leading-[1.7] text-fg-faint">
          {pk.stages} Benchmarks are PB campaign-history quartiles (p25 ·
          median · p75); the dot is this campaign. Click the pencil to rename
          an alias.
        </div>
      </div>

      {/* rotation imbalance: the winner is underfed */}
      {imbalance && (
        <WarnStrip kind="rotation" severity="info">
          {imbalance}
        </WarnStrip>
      )}

      {/* same ad, different rooms */}
      {matrix && matrix.platforms.length > 0 && (
        <div>
          <SectionHead right="per-platform splits · same sync">
            Same ad, different rooms: is the winner winning everywhere?
          </SectionHead>
          <PlatformMatrix
            matrix={matrix}
            judged={judged}
            objective={objective}
            benches={benches}
            perf={perf}
          />
        </div>
      )}

      {/* reporting strip */}
      <div>
        <SectionHead right="defensible numbers, caveats attached">
          For the report: campaign totals
        </SectionHead>
        <Card className="overflow-hidden p-0">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
            <ReportTile
              label="Spend"
              value={formatCurrencyCompact(totals.spend)}
              sub={win === "7d" ? "last 7 days" : "flight to date"}
              verdict={SCALE_METRIC_NOTE}
            />
            <ReportTile
              label="Impressions"
              value={formatNumberCompact(totals.impressions)}
              sub={`${formatNumberCompact(totals.clicks)} clicks`}
              verdict={SCALE_METRIC_NOTE}
            />
            <ReportTile
              label="Frequency"
              value={formatTimes(totals.frequency)}
              sub={win === "7d" ? "last 7 days" : "flight to date"}
              verdict={
                <VerdictWord
                  value={totals.frequency}
                  bench={benches.frequency}
                  size={8.5}
                  metric="frequency"
                />
              }
            />
            <ReportTile
              label="Completion"
              value={formatRate(totals.completion_rate)}
              sub={
                cov.completion.length > 0
                  ? `${covNames(cov.completion)} only`
                  : undefined
              }
              verdict={
                <VerdictWord
                  value={totals.completion_rate}
                  bench={benches.completion_rate}
                  size={8.5}
                  metric="completion_rate"
                />
              }
            />
            <ReportTile
              label="Engagement"
              value={formatRate(totals.engagement_rate)}
              sub={
                cov.engagement.length > 0
                  ? `${covNames(cov.engagement)} only`
                  : undefined
              }
              verdict={
                <VerdictWord
                  value={totals.engagement_rate}
                  bench={benches.engagement_rate}
                  size={8.5}
                />
              }
            />
            <ReportTile
              label="CPM"
              value={totals.cpm != null ? formatMoney(totals.cpm) : "—"}
              verdict={
                <VerdictWord
                  value={totals.cpm}
                  bench={benches.cpm}
                  size={8.5}
                  metric="cpm"
                />
              }
            />
          </div>
          {objective !== "awareness" && (
            <div className="grid grid-cols-2 border-t border-line-soft sm:grid-cols-3 lg:grid-cols-6">
              <ReportTile
                label="Conversions"
                value={formatNumberCompact(Math.round(totals.conversions))}
                verdict={
                  reportConv.source === "platform"
                    ? REPORT_THIS_NOTE
                    : SCALE_METRIC_NOTE
                }
              />
              <ReportTile
                label="CPA"
                value={totals.cpa != null ? formatMoney(totals.cpa) : "—"}
                sub={totals.cpa == null ? "awaiting first result" : undefined}
                verdict={
                  <VerdictWord
                    value={totals.cpa}
                    bench={benches.cpa}
                    size={8.5}
                    metric="cpa"
                  />
                }
              />
              <ReportTile
                label="CPC"
                value={
                  totals.clicks > 0
                    ? formatMoney(totals.spend / totals.clicks)
                    : "—"
                }
                verdict={
                  <VerdictWord
                    value={totals.clicks > 0 ? totals.spend / totals.clicks : null}
                    bench={benches.cpc}
                    size={8.5}
                    metric="cpc"
                  />
                }
              />
              <ReportTile
                label="Conv. rate"
                value={
                  totals.clicks > 0
                    ? formatRate(totals.conversions / totals.clicks)
                    : "—"
                }
                verdict={
                  <VerdictWord
                    value={
                      totals.clicks > 0
                        ? totals.conversions / totals.clicks
                        : null
                    }
                    bench={benches.conversion_rate}
                    size={8.5}
                  />
                }
              />
              <ReportTile
                label="Clicks"
                value={formatNumberCompact(totals.clicks)}
                sub={
                  totals.ctr != null ? `CTR ${formatRate(totals.ctr)}` : undefined
                }
                verdict={SCALE_METRIC_NOTE}
              />
              {ga4?.has_ga4 ? (
                <ReportTile
                  label={
                    <Glossary termKey="sessions_arrival" variant="icon">
                      Sessions (GA4)
                    </Glossary>
                  }
                  value={formatNumberCompact(ga4.total_sessions)}
                  sub={
                    reportArrival != null
                      ? `${Math.round(reportArrival)}% of clicks arrive${
                          reportArrival >= 100 ? " (over 100% is normal)" : ""
                        }`
                      : undefined
                  }
                  verdict={SCALE_METRIC_NOTE}
                />
              ) : (
                <ReportTile
                  label="CTR"
                  value={formatRate(totals.ctr)}
                  verdict={
                    <VerdictWord
                      value={totals.ctr}
                      bench={benches.ctr}
                      size={8.5}
                      metric="ctr"
                    />
                  }
                />
              )}
            </div>
          )}
        </Card>
      </div>

      {/* after the click */}
      <AfterTheClick ga4={ga4} perf={perf} onTab={onTab} />

      {/* depth drawer */}
      <LongTablesDrawer code={code} objective={objective} />
    </div>
  );
}
