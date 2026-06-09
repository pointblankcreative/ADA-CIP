"use client";

/**
 * Project detail shell — v0.3 structure.
 *
 * Tabs: Summary (default, verdict-first) / Pacing / Performance /
 * Diagnostics. Settings lives behind the gear icon; the old Alerts tab
 * is folded into Summary. The shell renders on tokens (light theme);
 * the not-yet-migrated tab bodies (pacing / performance / diagnostics /
 * settings) are pinned dark until their re-skin phases land.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import {
  ArrowLeft,
  Gauge,
  BarChart3,
  Activity,
  ScanLine,
  Settings2,
  Clock,
} from "lucide-react";
import { api, type Alert, type Project } from "@/lib/api";
import { computeFlight, verdict } from "@/lib/flight";
import { CodeChip, IconBtn } from "@/components/ui";
import { cn, formatCurrency, formatFlightDay } from "@/lib/utils";
import { SummaryTab } from "./summary-tab";
import { PacingTab } from "./pacing-tab";
import { PerformanceTab } from "./performance-tab";
import { SettingsTab } from "./settings-tab";
import { DiagnosticsTab } from "./diagnostics-tab";

const TABS = [
  { id: "summary", label: "Summary", icon: ScanLine },
  { id: "pacing", label: "Pacing", icon: Gauge },
  { id: "performance", label: "Performance", icon: BarChart3 },
  { id: "diagnostics", label: "Diagnostics", icon: Activity },
] as const;

type TabId = (typeof TABS)[number]["id"] | "settings";

function isUnprovisioned(p: Project): boolean {
  return !p.net_budget || p.net_budget === 0;
}

/**
 * Derive flight halves for `formatFlightDay` from a Project. flight_total_days
 * is `end - start + 1` (inclusive of both endpoints, matching how the
 * diagnostic engine counts), and flight_day is computed from today against
 * start_date. We pass daysRemaining straight through so the helper can prefer
 * it over the derived value (it's the source of truth — accounts for project
 * timezone the same way the API does).
 */
function buildFlightDayInput(p: Project) {
  const startMs = Date.parse(p.start_date + "T00:00:00");
  const endMs = Date.parse(p.end_date + "T00:00:00");
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const todayMs = today.getTime();

  const dayMs = 1000 * 60 * 60 * 24;
  const flightTotalDays =
    isFinite(startMs) && isFinite(endMs)
      ? Math.max(1, Math.round((endMs - startMs) / dayMs) + 1)
      : null;
  const flightDay =
    isFinite(startMs) && flightTotalDays != null
      ? Math.min(
          flightTotalDays,
          Math.max(1, Math.floor((todayMs - startMs) / dayMs) + 1),
        )
      : null;

  return {
    flightDay,
    flightTotalDays,
    daysRemaining: p.days_remaining,
  };
}

export default function ProjectDetailPage() {
  const params = useParams<{ code: string }>();
  const code = params.code;
  const [project, setProject] = useState<Project | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [activeTab, setActiveTab] = useState<TabId>("summary");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setActiveTab("summary");
    setLoading(true);
    api.projects
      .get(code)
      .then(setProject)
      .catch(() => setProject(null))
      .finally(() => setLoading(false));
    api.alerts
      .list({ project_code: code, limit: 50 })
      .then(setAlerts)
      .catch(() => setAlerts([]));
  }, [code]);

  const f = useMemo(
    () => (project ? computeFlight(project) : null),
    [project]
  );
  const v = useMemo(
    () => (project && f ? verdict(project, f) : null),
    [project, f]
  );
  const unprovisioned = project ? isUnprovisioned(project) : false;

  return (
    <div className="flex min-h-[calc(100vh-58px)] flex-col">
      {/* Header — sticky under the top bar */}
      <header className="sticky top-[58px] z-30 border-b-2 border-line-soft bg-surface-page">
        <div className="mx-auto max-w-[1340px] px-5 pt-2 sm:px-7">
          <Link
            href="/"
            className="inline-flex items-center gap-[7px] py-1 font-mono text-[11px] uppercase tracking-[0.08em] text-fg-muted transition-colors hover:text-fg"
          >
            <ArrowLeft className="h-3.5 w-3.5" /> Flightdeck
          </Link>

          <div className="mt-2 flex flex-wrap items-start justify-between gap-5">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2.5">
                <CodeChip accent>{code}</CodeChip>
                {loading ? (
                  <div className="h-6 w-48 animate-pulse rounded bg-surface-sunken" />
                ) : (
                  <h1 className="text-[22px] font-extrabold tracking-tight text-fg sm:text-[25px]">
                    {project?.project_name || `Project ${code}`}
                  </h1>
                )}
                {v && !unprovisioned && (
                  <span
                    className="inline-flex items-center gap-1.5 font-mono text-xs font-bold tracking-[0.04em]"
                    style={{ color: v.tone }}
                  >
                    <span
                      className="h-[7px] w-[7px] rounded-full"
                      style={{ backgroundColor: v.tone }}
                    />
                    {v.word}
                  </span>
                )}
                {project && !unprovisioned && <RetrospectivePicker code={code} />}
              </div>
              {project && !unprovisioned && (
                <div className="mt-2 flex flex-wrap items-center gap-x-[18px] gap-y-0.5 font-mono text-[11px] tracking-[0.04em] text-fg-muted">
                  {project.client_name && (
                    <span className="text-fg-meta">{project.client_name}</span>
                  )}
                  <span>
                    Budget {formatCurrency(project.net_budget)}
                    {project.currency ? ` ${project.currency}` : ""}
                  </span>
                  <span
                    className="cursor-help"
                    title="All platforms, all dates (data warehouse total)"
                  >
                    Spent {formatCurrency(project.total_spend)}
                  </span>
                  <span>{formatFlightDay(buildFlightDayInput(project), "combined")}</span>
                </div>
              )}
            </div>
            <div className="flex gap-2">
              <IconBtn
                icon={<Settings2 className="h-4 w-4" />}
                label="Settings"
                active={activeTab === "settings"}
                onClick={() =>
                  setActiveTab(activeTab === "settings" ? "summary" : "settings")
                }
              />
            </div>
          </div>

          {/* Unprovisioned banner */}
          {unprovisioned && (
            <div className="mt-4 flex flex-wrap items-center gap-4 rounded-md border-2 border-tint-warn bg-tint-warn px-5 py-3.5">
              <Settings2 className="h-5 w-5 flex-shrink-0 text-warn" />
              <div className="min-w-[220px] flex-1">
                <p className="text-sm font-semibold text-fg">
                  This campaign was detected automatically from ad platform data.
                </p>
                <p className="mt-0.5 text-xs text-fg-muted">
                  Set up project details to enable pacing alerts and media plan
                  tracking.
                </p>
              </div>
              <Link
                href={`/admin/projects/new?code=${code}`}
                className="flex-shrink-0 rounded-sm border-2 border-accent bg-accent px-4 py-2 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover"
              >
                Set Up Project
              </Link>
            </div>
          )}

          {/* Tabs */}
          <nav className="-mx-5 mt-3 flex gap-0.5 overflow-x-auto px-5 sm:mx-0 sm:px-0">
            {TABS.map(({ id, label, icon: Icon }) => {
              const active = activeTab === id;
              return (
                <button
                  key={id}
                  onClick={() => setActiveTab(id)}
                  className={cn(
                    "-mb-0.5 inline-flex shrink-0 items-center gap-[7px] whitespace-nowrap border-b-[2.5px] px-4 py-[11px] font-mono text-xs uppercase tracking-[0.06em] transition-colors duration-fast",
                    active
                      ? "border-accent font-semibold text-fg"
                      : "border-transparent font-medium text-fg-muted hover:text-fg"
                  )}
                >
                  <Icon className="h-3.5 w-3.5" />
                  {label}
                  {id === "summary" && alerts.length > 0 && (
                    <span className="font-mono text-[9.5px] font-bold text-danger">
                      {alerts.length}
                    </span>
                  )}
                </button>
              );
            })}
          </nav>
        </div>
      </header>

      {/* Tab content. Summary + Pacing are tokens (light); the remaining
          legacy tabs stay pinned dark until their re-skin phases land. */}
      {activeTab === "summary" || activeTab === "pacing" ? (
        <div className="mx-auto w-full max-w-[1340px] flex-1 px-5 py-6 pb-20 sm:px-7">
          {activeTab === "summary" && project && (
            <SummaryTab
              project={project}
              code={code}
              alerts={alerts}
              onTab={(t) => setActiveTab(t as TabId)}
            />
          )}
          {activeTab === "pacing" && <PacingTab code={code} />}
        </div>
      ) : (
        <div data-theme="dark" className="flex-1">
          <div className="mx-auto w-full max-w-[1340px] px-5 py-6 pb-20 sm:px-7">
            {activeTab === "performance" && <PerformanceTab code={code} />}
            {activeTab === "diagnostics" && <DiagnosticsTab code={code} />}
            {activeTab === "settings" && <SettingsTab code={code} />}
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * Entry-point button for Retrospective Mode (ADAC-51 commit 8).
 *
 * Renders a clock chip next to the verdict. Click opens a small popover
 * with a native date picker; submitting navigates to
 * /project/[code]/retrospective/[date].
 *
 * Default date is yesterday — the most common retro question is "what did
 * yesterday's pacing look like" and the daily pipeline runs at 5:30 AM/PM ET
 * so yesterday's row is reliably present in budget_tracking by the time
 * anyone asks.
 */
function yesterdayIso(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

function RetrospectivePicker({ code }: { code: string }) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [date, setDate] = useState<string>(yesterdayIso);
  const wrapperRef = useRef<HTMLDivElement>(null);

  // Click-outside-to-close. We listen on mousedown (not click) so the
  // popover dismisses before any nested button handlers run.
  useEffect(() => {
    if (!open) return;
    const handler = (event: MouseEvent) => {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const submit = () => {
    if (!date) return;
    setOpen(false);
    router.push(`/project/${code}/retrospective/${date}`);
  };

  return (
    <div ref={wrapperRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={cn(
          "flex h-7 items-center gap-1 rounded-sm px-2 font-mono text-[11px] uppercase tracking-[0.06em] transition-colors",
          open
            ? "bg-tint-warn text-warn"
            : "text-fg-muted hover:bg-surface-sunken hover:text-fg"
        )}
        aria-label="View historical snapshot"
        aria-expanded={open}
        title="View historical snapshot"
      >
        <Clock className="h-3.5 w-3.5" />
        <span className="hidden sm:inline">History</span>
      </button>
      {open && (
        <div className="absolute left-0 top-full z-30 mt-2 w-64 rounded-md border-2 border-line bg-surface-card p-3 shadow-soft">
          <p className="label text-[10px]">View as of date</p>
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") submit();
            }}
            autoFocus
            className="mt-2 w-full rounded-sm border-2 border-line bg-surface-sunken px-2 py-1.5 text-sm text-fg outline-none focus:border-warn"
          />
          <div className="mt-3 flex items-center justify-end gap-2">
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded-sm px-2 py-1 text-xs text-fg-muted hover:text-fg"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={submit}
              disabled={!date}
              className="rounded-sm border-2 border-tint-warn bg-tint-warn px-3 py-1 text-xs font-bold text-warn hover:opacity-80 disabled:opacity-50"
            >
              View snapshot
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
