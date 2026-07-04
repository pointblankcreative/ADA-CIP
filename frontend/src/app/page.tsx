"use client";

/**
 * Flightdeck — triage-first campaign board, replacing the Overview grid.
 * Visual weight = urgency: portfolio pulse, then exceptions, then every
 * flight as a dense row. All figures derive client-side from the existing
 * /api/projects/ payload via lib/flight.ts — no backend changes.
 */
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { RefreshCw, Zap } from "lucide-react";
import { api, type Project } from "@/lib/api";
import { computeFlight, verdict } from "@/lib/flight";
import { Card } from "@/components/card";
import { Label, Btn } from "@/components/ui";
import { PortfolioPulse } from "@/components/flightdeck/portfolio-pulse";
import {
  AttentionFeature,
  AttentionTile,
} from "@/components/flightdeck/attention";
import {
  FlightRow,
  FlightRowHeader,
} from "@/components/flightdeck/flight-row";
import {
  Segmented,
  SortSelect,
  type SortKey,
} from "@/components/flightdeck/controls";
import { OrphanPanel } from "@/components/orphan-panel";
import { SignalsPanel } from "@/components/signals/signals-panel";
import { SyncStatus } from "@/components/sync-status";
import { useIntro } from "@/components/intro/intro-provider";
import { cn } from "@/lib/utils";

export default function FlightdeckPage() {
  const router = useRouter();
  const { signalReady } = useIntro();
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<"active" | "ended">("active");
  const [sort, setSort] = useState<SortKey>("attention");
  // campaign code hovered in the Signals orbit — glows the matching row
  const [signalHover, setSignalHover] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const data = await api.projects.list();
      setProjects(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load campaigns");
    } finally {
      setLoading(false);
      // Cold-load splash: the project list is the Flightdeck's readiness.
      signalReady();
    }
  };

  useEffect(() => {
    load();
  }, []);

  const onOpen = (code: string) => router.push(`/project/${code}`);

  const active = useMemo(
    () => projects.filter((p) => p.status === "active" && !p.recently_ended),
    [projects]
  );
  const ended = useMemo(
    () => projects.filter((p) => p.status !== "active" || p.recently_ended),
    [projects]
  );

  // Needs attention: live flights off plan, worst first.
  const issues = useMemo(() => {
    return active
      .map((p) => ({ p, f: computeFlight(p) }))
      .filter(
        ({ f }) =>
          !f.noData &&
          (f.status.includes("critical") || f.status.includes("warning"))
      )
      .map((x) => ({
        ...x,
        v: verdict(x.p, x.f),
        sev: x.f.status.includes("critical") ? 2 : 1,
        dev: Math.abs((x.p.pacing_percentage ?? 100) - 100),
      }))
      .sort((a, b) => b.sev - a.sev || b.dev - a.dev);
  }, [active]);

  const list = tab === "active" ? active : ended;
  const sorted = useMemo(() => {
    const arr = list.map((p) => ({ p, f: computeFlight(p) }));
    const cmp: Record<
      SortKey,
      (a: (typeof arr)[number], b: (typeof arr)[number]) => number
    > = {
      attention: (a, b) =>
        Number(a.f.noData) - Number(b.f.noData) ||
        Math.abs((b.p.pacing_percentage ?? 100) - 100) -
          Math.abs((a.p.pacing_percentage ?? 100) - 100),
      pace: (a, b) =>
        (b.p.pacing_percentage ?? 0) - (a.p.pacing_percentage ?? 0),
      budget: (a, b) => (b.p.net_budget ?? 0) - (a.p.net_budget ?? 0),
      days: (a, b) => (a.p.days_remaining ?? 0) - (b.p.days_remaining ?? 0),
      name: (a, b) => a.p.project_name.localeCompare(b.p.project_name),
    };
    return arr.sort(cmp[sort]).map((x) => x.p);
  }, [list, sort]);

  // Freshest updated_at in the payload — feeds the sync-status stamp.
  const lastSync = useMemo(() => {
    const stamps = projects
      .map((p) => p.updated_at)
      .filter(Boolean)
      .sort();
    return stamps[stamps.length - 1] ?? null;
  }, [projects]);

  return (
    <div className="mx-auto max-w-[1340px] px-5 pb-20 pt-7 sm:px-7">
      {/* header */}
      <div className="mb-6 flex flex-wrap items-end justify-between gap-5">
        <div>
          <h1 className="display text-[38px] text-fg sm:text-[44px]">
            Flightdeck
          </h1>
          <p className="mt-3 max-w-[480px] text-sm text-fg-muted">
            What needs you, first. Then every flight, paced in real time — no
            vanity metrics, just whether the money&apos;s landing.
          </p>
        </div>
        <div className="flex items-center gap-2.5">
          {loading ? (
            <span className="font-mono text-[10.5px] uppercase text-fg-faint">
              Syncing…
            </span>
          ) : (
            <SyncStatus lastUpdated={lastSync} />
          )}
          <Btn
            variant="outline"
            size="sm"
            onClick={load}
            disabled={loading}
            icon={
              <RefreshCw className={cn("h-3.5 w-3.5", loading && "animate-spin")} />
            }
          >
            {loading ? "Syncing" : "Refresh"}
          </Btn>
        </div>
      </div>

      {error && (
        <Card className="mb-6 border-tint-danger bg-tint-danger text-sm text-danger">
          {error}
        </Card>
      )}

      {loading && projects.length === 0 ? (
        <FlightdeckSkeleton />
      ) : (
        <>
          <PortfolioPulse active={active} onOpen={onOpen} />

          {/* Signals — the book's health, by sight and (opt-in) sound */}
          <SignalsPanel
            projects={active}
            onOpen={onOpen}
            onHover={setSignalHover}
          />

          {/* Needs attention */}
          {issues.length > 0 && (
            <div className="mt-[30px]">
              <div className="mb-4 flex items-center gap-3">
                <Zap className="h-4 w-4 text-danger" />
                <Label className="text-fg-secondary">Needs Attention</Label>
                <span className="rounded-pill bg-tint-danger px-2 py-px font-mono text-[11px] text-danger">
                  {issues.length}
                </span>
                <div className="h-px flex-1 bg-line-soft" />
              </div>
              <div className="grid grid-cols-[repeat(auto-fit,minmax(280px,1fr))] gap-3.5">
                <AttentionFeature
                  p={issues[0].p}
                  f={issues[0].f}
                  v={issues[0].v}
                  onOpen={onOpen}
                />
                {issues.slice(1, 4).map(({ p, f, v }) => (
                  <AttentionTile
                    key={p.project_code}
                    p={p}
                    f={f}
                    v={v}
                    onOpen={onOpen}
                  />
                ))}
              </div>
            </div>
          )}

          {/* Flights */}
          <div className="mt-[34px]">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3.5">
              <Segmented
                value={tab}
                onChange={(id) => setTab(id as "active" | "ended")}
                options={[
                  { id: "active", label: "Active", count: active.length },
                  { id: "ended", label: "Ended", count: ended.length },
                ]}
              />
              {tab === "active" && (
                <SortSelect value={sort} onChange={setSort} />
              )}
            </div>

            <FlightRowHeader ended={tab === "ended"} />

            <div className="flex flex-col gap-2">
              {sorted.length === 0 ? (
                <Card className="px-6 py-11 text-center text-sm text-fg-faint">
                  {tab === "active"
                    ? "No active campaigns."
                    : "No ended campaigns yet."}
                </Card>
              ) : (
                sorted.map((p, i) => (
                  <FlightRow
                    key={p.project_code}
                    p={p}
                    onOpen={onOpen}
                    delay={i}
                    glow={signalHover === p.project_code}
                  />
                ))
              )}
            </div>
          </div>
        </>
      )}

      {/* Unconfigured spend (orphan project codes in fact_* not in dim_projects) */}
      <OrphanPanel />
    </div>
  );
}

function FlightdeckSkeleton() {
  return (
    <div className="space-y-3.5">
      <p className="text-xs text-fg-muted">Loading your campaigns…</p>
      <Card className="animate-pulse">
        <div className="h-4 w-40 rounded bg-surface-sunken" />
        <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-12 rounded bg-surface-sunken" />
          ))}
        </div>
      </Card>
      {Array.from({ length: 5 }).map((_, i) => (
        <Card key={i} className="animate-pulse py-[15px]">
          <div className="h-4 w-2/3 rounded bg-surface-sunken" />
          <div className="mt-2.5 h-2.5 w-full rounded bg-surface-sunken" />
        </Card>
      ))}
    </div>
  );
}
