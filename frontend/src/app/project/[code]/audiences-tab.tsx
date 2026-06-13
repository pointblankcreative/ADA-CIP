"use client";

/**
 * Audiences tab — the Electorate. Every ad set is its own electorate.
 *
 * Ported from the Claude Design prototype (tab-audiences.jsx).
 * Collapsed = the Crosstab: an audience × creative resonance matrix
 * read through a KPI lens (the objective sets the default ✱).
 * Expanded = the dossier: how this audience responds vs PB history,
 * plus its frequency trend.
 *
 * v2 (Phase 19): the dossier's WHO THIS IS persona and SATURATION bar
 * are live, fed by the Meta targeting-spec sync. Both render only when
 * the backend has data (persona / saturation non-null) — non-Meta ad
 * sets keep the v1 layout with the slots invisible.
 */
import { useEffect, useMemo, useState } from "react";
import { Users } from "lucide-react";
import {
  api,
  type AudienceMatrixCell,
  type AudienceMatrixResponse,
  type BenchmarkResponse,
  type CreativeRotationResponse,
  type MatrixAudience,
  type ObjectiveType,
  type PerformanceResponse,
} from "@/lib/api";
import {
  buildBenches,
  formatMoney,
  formatRate,
  formatTimes,
  judgeCreative,
  lensesFor,
  quartileRead,
  rankCreatives,
  resolveCell,
  type CreativeBenches,
  type CreativeVerdict,
  type LensId,
  type QuartileBench,
} from "@/lib/creative";
import {
  CreativeVerdictChip,
  LensSwitch,
  MatrixCell,
  QuartileBar,
  Spark,
  WarnStrip,
} from "@/components/perf/primitives";
import { Card } from "@/components/card";
import { Eyebrow } from "@/components/ui";
import { PlatformIcon } from "@/components/platform-icon";
import { SyncStatus } from "@/components/sync-status";
import {
  cn,
  formatCurrencyCompact,
  formatNumberCompact,
  platformLabel,
} from "@/lib/utils";
import { statusWord } from "@/lib/viz/health-core";

/* ── Per-audience read: status from the primary-KPI quartile ─────── */

type AudienceStatus = "STRONG" | "WATCH" | "ACT" | "NO DATA";

function statusVar(s: AudienceStatus): string {
  if (s === "STRONG") return "var(--ok)";
  if (s === "WATCH") return "var(--warn)";
  if (s === "ACT") return "var(--danger)";
  return "var(--text-faint)";
}

interface AudienceRead {
  status: AudienceStatus;
  needsDecision: boolean;
  latestFreq: number | null;
  freqHot: boolean;
  /** Impression-weighted hook across this audience's cells — the
   *  contract doesn't carry an audience-level hook_rate. */
  hookRate: number | null;
}

function readAudience(
  a: MatrixAudience,
  objective: ObjectiveType,
  benches: CreativeBenches,
  cells: Record<string, AudienceMatrixCell> | undefined
): AudienceRead {
  const latestFreq = a.frequency_trend?.length
    ? a.frequency_trend[a.frequency_trend.length - 1]
    : a.frequency;
  const freqHot = latestFreq != null && latestFreq > 4;

  const primary = objective === "awareness" ? a.completion_rate : a.cpa;
  const bench =
    objective === "awareness" ? benches.completion_rate : benches.cpa;
  const read = quartileRead(primary, bench);

  let status: AudienceStatus;
  if (read == null) status = "NO DATA";
  else if (read.rank === 0) status = "ACT";
  else if (read.rank === 1) status = "WATCH";
  else status = "STRONG";
  // Hot frequency is actionable on its own, benchmark or not.
  if (freqHot && (status === "STRONG" || status === "NO DATA")) {
    status = "WATCH";
  }

  let hookNum = 0;
  let hookDen = 0;
  for (const cell of Object.values(cells ?? {})) {
    if (cell.hook_rate != null && cell.impressions > 0) {
      hookNum += cell.hook_rate * cell.impressions;
      hookDen += cell.impressions;
    }
  }

  return {
    status,
    /* NO DATA is an unknown, not a decision: counting it inflated the
       hero ("8 need a decision") on campaigns with no benchmarks yet. */
    needsDecision: status === "WATCH" || status === "ACT",
    latestFreq,
    freqHot,
    hookRate: hookDen > 0 ? hookNum / hookDen : null,
  };
}

/* ── The response stack — how this audience behaves vs PB history ── */

interface DossierRow {
  label: string;
  text: string | null;
  value: number | null;
  bench?: QuartileBench;
  /** Rendered instead of a quartile bar (e.g. Search CTR). */
  tag?: string;
}

function buildDossierRows(
  a: MatrixAudience,
  objective: ObjectiveType,
  benches: CreativeBenches,
  hookRate: number | null
): DossierRow[] {
  const rows: DossierRow[] = [];
  if (hookRate != null) {
    rows.push({
      label: "HOOK",
      text: formatRate(hookRate),
      value: hookRate,
      bench: benches.hook_rate,
    });
  }
  if ((a.role ?? "").toLowerCase() === "search") {
    rows.push({
      label: "CTR",
      text: a.ctr != null ? formatRate(a.ctr) : null,
      value: null,
      tag: "NOT COMPARABLE",
    });
  } else {
    rows.push({
      label: "CTR",
      text: a.ctr != null ? formatRate(a.ctr) : null,
      value: a.ctr,
      bench: benches.ctr,
    });
  }
  if (objective === "awareness") {
    if (a.completion_rate != null) {
      rows.push({
        label: "COMPLETION",
        text: formatRate(a.completion_rate),
        value: a.completion_rate,
        bench: benches.completion_rate,
      });
    }
    if (a.engagement_rate != null) {
      rows.push({
        label: "ENG",
        text: formatRate(a.engagement_rate),
        value: a.engagement_rate,
        bench: benches.engagement_rate,
      });
    }
  } else {
    if (a.cpa != null) {
      rows.push({
        label: "CPA",
        text: formatMoney(a.cpa),
        value: a.cpa,
        bench: benches.cpa,
      });
    } else {
      rows.push({
        label: "CPA",
        text: null,
        value: null,
        tag: a.spend > 0 && a.conversions === 0 ? "NO RESULTS RECORDED" : "NO DATA",
      });
    }
  }
  return rows;
}

/* ── SaturationBar — how much of the pool the ads have reached ──────
   The prototype's AUSat bar: fill = saturation, a quiet tick at the 80%
   guardrail, danger tint once the pool is mostly spent. */

function SaturationBar({
  saturation,
  poolSize,
}: {
  saturation: number;
  poolSize: number | null;
}) {
  const pct = Math.min(saturation, 1) * 100;
  const hot = saturation > 0.8;
  const tone = hot ? "var(--danger)" : "var(--accent)";
  return (
    <div className="max-w-[200px]">
      <div className="relative h-[5px] overflow-hidden rounded-full bg-surface-card">
        <div
          className="h-full rounded-full"
          style={{ width: `${pct}%`, background: tone }}
        />
        {/* the 80% guardrail tick */}
        <div
          className="absolute bottom-0 top-0 w-px"
          style={{ left: "80%", background: "var(--text-faint)" }}
        />
      </div>
      <div className="mt-1 flex items-baseline gap-1.5">
        <span
          className="tnum font-mono text-[10px] font-bold"
          style={{ color: tone }}
        >
          {Math.round(saturation * 100)}%
        </span>
        {poolSize != null && (
          <span className="font-mono text-[8.5px] text-fg-faint">
            of {formatNumberCompact(poolSize)} pool reached
          </span>
        )}
      </div>
      {hot && (
        <div className="mt-1 font-mono text-[8.5px] text-danger">
          Most of this pool has already seen the ads: spend here buys
          repetition, not new people.
        </div>
      )}
    </div>
  );
}

function Dossier({
  a,
  read,
  rows,
}: {
  a: MatrixAudience;
  read: AudienceRead;
  rows: DossierRow[];
}) {
  return (
    <div className="grid items-start gap-5 border-t border-line-soft bg-surface-sunken py-3.5 pl-[37px] pr-[18px] sm:grid-cols-[1.1fr_0.9fr]">
      <div>
        <div className="mb-1.5 font-mono text-[7.5px] tracking-[0.12em] text-fg-faint">
          HOW THEY RESPOND · VS PB HISTORY
        </div>
        <div className="flex flex-col gap-1.5">
          {rows.map((r) => (
            <div
              key={r.label}
              className="grid grid-cols-[78px_56px_1fr] items-center gap-2"
            >
              <span className="font-mono text-[8.5px] text-fg-faint">
                {r.label}
              </span>
              <span className="tnum font-mono text-xs font-bold text-fg">
                {r.text ?? "—"}
              </span>
              {r.tag ? (
                <span className="font-mono text-[8px] text-fg-faint">
                  {r.tag}
                </span>
              ) : (
                <QuartileBar value={r.value} bench={r.bench ?? null} width={100} />
              )}
            </div>
          ))}
        </div>
        {/* CTR-trend slot: the matrix contract doesn't carry a
            per-audience CTR series yet. When audiences[].ctr_trend
            lands, render a Spark here mirroring the frequency block. */}
      </div>
      <div className="flex flex-col gap-2.5">
        <div>
          <div className="mb-1.5 font-mono text-[7.5px] tracking-[0.12em] text-fg-faint">
            FREQUENCY · AVG IMPRESSIONS / PERSON · LAST SYNCS
          </div>
          <div className="flex items-center gap-2.5">
            <Spark
              data={a.frequency_trend}
              width={88}
              height={16}
              color={read.freqHot ? "var(--danger)" : "var(--text-muted)"}
            />
            <span
              className={cn(
                "font-mono text-[10px] font-semibold",
                read.freqHot ? "text-danger" : "text-fg-muted"
              )}
            >
              {read.latestFreq != null
                ? `now ${formatTimes(read.latestFreq)}`
                : "—"}
            </span>
          </div>
          {read.freqHot && (
            <div className="mt-1 font-mono text-[8.5px] text-danger">
              Above the 4× guardrail: this room is seeing the ads a lot.
            </div>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {a.role && (
            <span className="rounded-xs border border-line px-1.5 py-0.5 font-mono text-[9px] font-semibold uppercase tracking-[0.1em] text-fg-muted">
              {a.role}
            </span>
          )}
          <span className="font-mono text-[8.5px] text-fg-faint">
            {platformLabel(a.platform_id)} ad set
          </span>
        </div>
        {/* WHO THIS IS — the persona from the platform targeting spec.
            Invisible when the sync has nothing (non-Meta ad sets). */}
        {a.persona != null && (
          <div>
            <div className="mb-1 font-mono text-[7.5px] tracking-[0.12em] text-fg-faint">
              WHO THIS IS · FROM PLATFORM TARGETING
            </div>
            <p className="text-xs leading-[1.55] text-fg-secondary">
              {a.persona}
            </p>
          </div>
        )}
        {/* SATURATION — pool penetration. Same rule: absent data, no UI. */}
        {a.saturation != null && (
          <div>
            <div className="mb-1.5 font-mono text-[7.5px] tracking-[0.12em] text-fg-faint">
              SATURATION
            </div>
            <SaturationBar saturation={a.saturation} poolSize={a.pool_size} />
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Empty state — calm, not an error ────────────────────────────── */

function AudienceLevelAwaiting() {
  return (
    <Card className="flex flex-col items-center gap-3.5 px-6 py-14 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-md border-[1.5px] border-tint-info bg-tint-info">
        <Users className="h-[22px] w-[22px] text-info" />
      </div>
      <div>
        <h3 className="text-lg font-bold text-fg">
          Ad-level data not available for this campaign yet
        </h3>
        <p className="mx-auto mt-2 max-w-[460px] text-[13.5px] leading-relaxed text-fg-muted">
          Audience reporting needs ad-set sync confirmed for this campaign.
          Platform totals keep flowing in Pacing meanwhile: nothing is lost,
          this page just won&apos;t guess.
        </p>
      </div>
    </Card>
  );
}

/* ── The tab ─────────────────────────────────────────────────────── */

export function AudiencesTab({
  code,
  onTab,
}: {
  code: string;
  onTab: (tab: string) => void;
}) {
  const [audMatrix, setAudMatrix] = useState<AudienceMatrixResponse | null>(
    null
  );
  const [rotation, setRotation] = useState<CreativeRotationResponse | null>(
    null
  );
  const [bench, setBench] = useState<BenchmarkResponse | null>(null);
  const [perf, setPerf] = useState<PerformanceResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [lensRaw, setLens] = useState<LensId | null>(null);
  const [userOpen, setUserOpen] = useState<Set<string> | null>(null);

  useEffect(() => {
    setLoading(true);
    setUserOpen(null);
    setLens(null);
    Promise.all([
      api.audiences.matrix(code).catch(() => null),
      api.creative.rotation(code, "flight").catch(() => null),
      api.benchmarks.get(code).catch(() => null),
      api.performance.get(code).catch(() => null),
    ])
      .then(([aud, rot, b, p]) => {
        setAudMatrix(aud);
        setRotation(rot);
        setBench(b);
        setPerf(p);
      })
      .finally(() => setLoading(false));
  }, [code]);

  const objective: ObjectiveType =
    rotation?.objective ?? perf?.objective_type ?? "mixed";
  const benches = useMemo(() => buildBenches(bench), [bench]);

  // Creative verdicts echo the Creative page; columns follow its rank.
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
  const verdicts = useMemo(() => {
    const m = new Map<string, CreativeVerdict>();
    for (const j of judged) m.set(j.creative.variant, j.verdict);
    return m;
  }, [judged]);

  const reads = useMemo(() => {
    const m = new Map<string, AudienceRead>();
    if (!audMatrix) return m;
    for (const a of audMatrix.audiences) {
      m.set(a.id, readAudience(a, objective, benches, audMatrix.cells[a.id]));
    }
    return m;
  }, [audMatrix, objective, benches]);

  // The CPM lens belongs to the creative × platform matrix only.
  const lenses = useMemo(
    () => lensesFor(objective).filter((l) => !l.platformMatrixOnly),
    [objective]
  );
  const lens =
    lensRaw && lenses.some((l) => l.id === lensRaw) ? lensRaw : lenses[0].id;
  const activeLens = lenses.find((l) => l.id === lens)!;
  const isPrimaryLens = !!activeLens.primary;

  if (loading) {
    return (
      <div className="space-y-4">
        <Card className="animate-pulse">
          <div className="h-9 w-72 rounded bg-surface-sunken" />
          <div className="mt-4 h-4 w-96 rounded bg-surface-sunken" />
        </Card>
        <Card className="h-72 animate-pulse" />
      </div>
    );
  }

  if (!audMatrix || audMatrix.audiences.length === 0) {
    return <AudienceLevelAwaiting />;
  }

  const audiences = audMatrix.audiences;
  const ids = audiences.map((a) => a.id);
  const openSet = userOpen ?? new Set(ids.length ? [ids[0]] : []);
  const everyOpen = ids.length > 0 && openSet.size === ids.length;
  const toggleOne = (id: string) => {
    const next = new Set(openSet);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setUserOpen(next);
  };

  // Column order: rotation rank first, then any matrix-only variants.
  const columns = [...audMatrix.creatives].sort((a, b) => {
    const ia = judged.findIndex((j) => j.creative.variant === a);
    const ib = judged.findIndex((j) => j.creative.variant === b);
    return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
  });
  /* First track needs a floor: at minmax(0,…) the audience-label column
     collapsed under 12 creative columns and the corner label overlapped
     the first header. */
  const cols = `minmax(190px,1.25fr) repeat(${columns.length}, minmax(96px, 110px)) 92px`;
  const needCount = audiences.filter((a) => reads.get(a.id)?.needsDecision)
    .length;

  /* Unattributed-spend guard: a completed flight whose per-line spend never
     attributed renders the whole crosstab as "—" under the active lens
     (e.g. cpa null in every cell). A full grid of dashes reads as broken,
     so when essentially no cell carries a real number we explain the gap
     instead of showing the empty matrix. Counted on the cells the contract
     actually delivered, under the lens the user is reading. */
  let cellsWithData = 0;
  let cellsPresent = 0;
  for (const a of audiences) {
    const row = audMatrix.cells[a.id];
    if (!row) continue;
    for (const variant of columns) {
      const cell = row[variant];
      if (!cell) continue; // missing key = creative doesn't run here, not a gap
      cellsPresent += 1;
      if (resolveCell(cell, lens, benches).kind === "value") cellsWithData += 1;
    }
  }
  /* Essentially empty = zero readable cells, or a stray handful (≤5% of the
     cells present). Above that the matrix earns its place — keep it visible. */
  const matrixEmpty =
    cellsPresent > 0 && cellsWithData <= Math.max(0, cellsPresent * 0.05);

  return (
    <div className="flex flex-col gap-4">
      {/* the call */}
      <Card className="p-6 sm:p-7">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <Eyebrow>The electorate · ad sets, judged individually</Eyebrow>
          {rotation && (
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap font-mono text-[10.5px] text-fg-faint">
              Data as of {rotation.as_of} ·
              <SyncStatus variant="compact" />
            </span>
          )}
        </div>
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="display mt-3 text-[34px] text-fg sm:text-[40px]">
              {audiences.length} audience{audiences.length === 1 ? "" : "s"}.{" "}
              <span className="text-accent-ink">
                {needCount} need{needCount === 1 ? "s" : ""} a decision.
              </span>
            </div>
            <p className="mt-2 max-w-[680px] text-sm leading-[1.55] text-fg-secondary">
              Every ad set is its own electorate: same ads, different rooms.
              Cells show <b>{lenses[0].label.toLowerCase()}</b>, the KPI this
              campaign&apos;s objective buys, coloured against PB history.
              Flip the lens to ask a different question; open a row for the
              full read.
            </p>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="font-mono text-[8.5px] tracking-[0.1em] text-fg-faint">
              ZOOM
            </span>
            {(["MATRIX", "ALL DOSSIERS"] as const).map((lbl) => {
              const active = lbl === "ALL DOSSIERS" ? everyOpen : !everyOpen;
              return (
                <button
                  key={lbl}
                  onClick={() => {
                    if (active) return;
                    setUserOpen(
                      lbl === "ALL DOSSIERS" ? new Set(ids) : new Set()
                    );
                  }}
                  className={cn(
                    "whitespace-nowrap rounded-xs border px-3 py-1 font-mono text-[9.5px] font-bold tracking-[0.08em] transition-colors duration-fast",
                    active
                      ? "border-accent-ink bg-tint-accent text-accent-ink"
                      : "border-line text-fg-muted hover:text-fg"
                  )}
                >
                  {lbl}
                </button>
              );
            })}
          </div>
        </div>
      </Card>

      {perf?.high_frequency_warning && (
        <WarnStrip kind="frequency">
          {perf.high_frequency_warning}{" "}
          <span className="text-fg-faint">
            Frequency = avg impressions per person reached; this flags the peak
            across the campaign&apos;s ad sets (reporting window set by the
            platform).
          </span>
        </WarnStrip>
      )}

      {/* Unattributed line spend: the crosstab structure is sound but has no
          per-cell result cost to show, so explain the wall of dashes rather
          than letting it read as broken. The matrix stays visible — its row
          reads (frequency, saturation, dossiers) still hold. */}
      {matrixEmpty && (
        <WarnStrip kind="no cell data" severity="info">
          Line-level spend isn&apos;t attributed for this flight yet, so
          per-cell {activeLens.label.toLowerCase()} is unavailable and the
          cells below read &ldquo;—&rdquo;. The row reads (frequency,
          saturation and each audience&apos;s dossier) still hold; the cells
          fill in once spend is attributed.
        </WarnStrip>
      )}

      {/* the matrix */}
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
          <div className="min-w-[680px]">
            <div
              className="grid items-stretch border-b-2 border-line"
              style={{ gridTemplateColumns: cols }}
            >
              <div className="flex items-end px-4 py-2.5">
                <span className="font-mono text-[8.5px] tracking-[0.12em] text-fg-faint">
                  AUDIENCE ↓ · CREATIVE →
                </span>
              </div>
              {columns.map((variant) => (
                <button
                  key={variant}
                  onClick={() => onTab("creative")}
                  title="Creative verdicts live on the Creative page"
                  className="border-l border-line-soft px-2 pb-2 pt-2.5 text-center transition-colors hover:bg-surface-sunken"
                >
                  <div className="text-[11.5px] font-extrabold leading-tight text-fg">
                    {variant}
                  </div>
                  {verdicts.has(variant) && (
                    <div className="mt-1">
                      <CreativeVerdictChip
                        verdict={verdicts.get(variant)!}
                        size="sm"
                      />
                    </div>
                  )}
                </button>
              ))}
              <div className="flex items-end justify-center border-l border-line-soft px-2 pb-2.5 pt-2.5">
                <span className="font-mono text-[8.5px] tracking-[0.1em] text-fg-faint">
                  VERDICT
                </span>
              </div>
            </div>
            {audiences.map((a, i) => {
              const read = reads.get(a.id)!;
              const open = openSet.has(a.id);
              const row = audMatrix.cells[a.id];
              return (
                <div key={a.id} className={cn(i > 0 && "border-t border-line-soft")}>
                  <div
                    role="button"
                    tabIndex={0}
                    onClick={() => toggleOne(a.id)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        toggleOne(a.id);
                      }
                    }}
                    className="grid cursor-pointer items-stretch"
                    style={{
                      gridTemplateColumns: cols,
                      background: open
                        ? "color-mix(in srgb, var(--accent) 4%, transparent)"
                        : undefined,
                    }}
                  >
                    <div className="flex min-w-0 items-center gap-2 px-4 py-2">
                      <span className="w-2.5 font-mono text-[9px] text-fg-faint">
                        {open ? "▾" : "▸"}
                      </span>
                      <PlatformIcon platformId={a.platform_id} size={22} />
                      <div className="min-w-0">
                        <div className="truncate text-[12.5px] font-bold text-fg">
                          {a.name}
                        </div>
                        <div className="truncate font-mono text-[8.5px] text-fg-faint">
                          {formatCurrencyCompact(a.spend)}
                          {a.role ? ` · ${a.role}` : ""}
                          {read.freqHot && read.latestFreq != null
                            ? ` · ${formatTimes(read.latestFreq)} avg impressions/person (latest sync)`
                            : ""}
                          {a.saturation != null
                            ? ` · ${Math.round(a.saturation * 100)}% saturated`
                            : ""}
                        </div>
                      </div>
                    </div>
                    {columns.map((variant) => (
                      <div
                        key={variant}
                        className="flex flex-col justify-center border-l border-line-soft"
                      >
                        <MatrixCell
                          cell={row?.[variant] ?? null}
                          lens={lens}
                          benches={benches}
                          fatigued={
                            read.freqHot && verdicts.get(variant) === "REFRESH"
                          }
                        />
                      </div>
                    ))}
                    <div className="flex items-center justify-center border-l border-line-soft">
                      <span
                        className="font-mono text-[8.5px] font-bold tracking-[0.1em]"
                        style={{ color: statusVar(read.status) }}
                      >
                        {statusWord(read.status)}
                      </span>
                    </div>
                  </div>
                  {open && (
                    <Dossier
                      a={a}
                      read={read}
                      rows={buildDossierRows(a, objective, benches, read.hookRate)}
                    />
                  )}
                </div>
              );
            })}
          </div>
        </div>
        {/* the read */}
        <div className="border-t-2 border-line bg-surface-sunken px-[18px] py-3">
          <span className="font-mono text-[9px] tracking-[0.1em] text-fg-faint">
            THE READ ·{" "}
          </span>
          <span className="text-xs leading-[1.5] text-fg-secondary">
            Cell = {activeLens.label.toLowerCase()} for that creative in that
            ad set, coloured against PB history.
            {isPrimaryLens
              ? " ✱ marks the campaign's primary KPI: set by the objective, not by us."
              : " Diagnostic lens: the verdict column still ranks on the primary KPI."}{" "}
            A &ldquo;—&rdquo; means that creative doesn&apos;t run in this ad
            set: sometimes a gap is a test worth running. Cells answer{" "}
            <i>which</i>; anything broken links into Diagnostics for{" "}
            <i>why</i>.
          </span>
        </div>
      </Card>

      <div className="font-mono text-[9px] leading-[1.7] text-fg-faint">
        Search CTR isn&apos;t comparable to social. Hook = 3-second views over
        impressions, weighted across the ad set&apos;s creatives. Verdict =
        the audience&apos;s {objective === "awareness" ? "completion" : "cost per result"}{" "}
        quartile vs PB history.
      </div>
    </div>
  );
}
