"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import {
  ArrowLeft,
  Gauge,
  BarChart3,
  AlertTriangle,
  Settings2,
  Activity,
  Clock,
} from "lucide-react";
import { api, type Project } from "@/lib/api";
import { PacingBadge } from "@/components/pacing-badge";
import { cn, formatCurrency, formatFlightDay } from "@/lib/utils";
import { PacingTab } from "./pacing-tab";
import { PerformanceTab } from "./performance-tab";
import { AlertsTab } from "./alerts-tab";
import { SettingsTab } from "./settings-tab";
import { DiagnosticsTab } from "./diagnostics-tab";

const TABS = [
  { id: "pacing", label: "Pacing", icon: Gauge },
  { id: "performance", label: "Performance", icon: BarChart3 },
  { id: "diagnostics", label: "Diagnostics", icon: Activity },
  { id: "alerts", label: "Alerts", icon: AlertTriangle },
  { id: "settings", label: "Settings", icon: Settings2 },
] as const;

type TabId = (typeof TABS)[number]["id"];

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
  const [activeTab, setActiveTab] = useState<TabId>("pacing");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.projects
      .get(code)
      .then(setProject)
      .catch(() => setProject(null))
      .finally(() => setLoading(false));
  }, [code]);

  const unprovisioned = project ? isUnprovisioned(project) : false;

  return (
    <div className="flex min-h-screen flex-col">
      {/* Top bar */}
      <header className="border-b border-slate-800 bg-surface px-4 py-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-3 pl-10 md:pl-0">
          <Link
            href="/"
            className="flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-md text-slate-500 transition-colors hover:bg-slate-800 hover:text-slate-300"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2 sm:gap-3">
              <span className="rounded bg-slate-800 px-2 py-0.5 font-mono text-xs text-slate-400">
                {code}
              </span>
              {loading ? (
                <div className="h-5 w-48 animate-pulse rounded bg-slate-700" />
              ) : (
                <h1 className="truncate text-base font-semibold text-white sm:text-lg">
                  {project?.project_name || `Project ${code}`}
                </h1>
              )}
              {project && !unprovisioned && (
                <PacingBadge percentage={project.pacing_percentage} totalSpend={project.total_spend} size="sm" />
              )}
              {/* Entry-point to Retrospective Mode (ADAC-51 commit 8). Hidden
                  for unprovisioned (orphan-discovered) projects since they
                  have no historical data yet. */}
              {project && !unprovisioned && (
                <RetrospectivePicker code={code} />
              )}
            </div>
            {project && !unprovisioned && (
              <div className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-0.5 text-xs text-slate-500">
                <span>Budget: {formatCurrency(project.net_budget)}{project.currency ? ` ${project.currency}` : ""}</span>
                <span>Spent: {formatCurrency(project.total_spend)}{project.currency ? ` ${project.currency}` : ""}</span>
                <span>{formatFlightDay(buildFlightDayInput(project), "combined")}</span>
              </div>
            )}
          </div>
        </div>

        {/* Unprovisioned banner */}
        {unprovisioned && (
          <div className="mt-4 flex items-center gap-4 rounded-lg border border-amber-500/30 bg-amber-500/10 px-5 py-3.5">
            <Settings2 className="h-5 w-5 flex-shrink-0 text-amber-400" />
            <div className="flex-1">
              <p className="text-sm font-medium text-amber-300">
                This campaign was detected automatically from ad platform data.
              </p>
              <p className="mt-0.5 text-xs text-amber-400/70">
                Set up project details to enable pacing alerts and media plan tracking.
              </p>
            </div>
            <Link
              href={`/admin/projects/new?code=${code}`}
              className="flex-shrink-0 rounded-md bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-500 transition-colors"
            >
              Set Up Project
            </Link>
          </div>
        )}

        {/* Tabs */}
        <nav className="mt-4 flex gap-1 overflow-x-auto -mx-4 px-4 sm:mx-0 sm:px-0">
          {TABS.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={cn(
                "flex shrink-0 items-center gap-1.5 rounded-md px-3 py-1.5 text-sm whitespace-nowrap transition-colors",
                activeTab === id
                  ? "bg-brand-600/15 text-brand-400 font-medium"
                  : "text-slate-400 hover:bg-slate-800 hover:text-slate-200"
              )}
            >
              <Icon className="h-3.5 w-3.5" />
              {label}
            </button>
          ))}
        </nav>
      </header>

      {/* Tab content */}
      <div className="flex-1 p-4 sm:p-6 lg:p-8">
        {activeTab === "pacing" && <PacingTab code={code} />}
        {activeTab === "performance" && <PerformanceTab code={code} />}
        {activeTab === "diagnostics" && <DiagnosticsTab code={code} />}
        {activeTab === "alerts" && <AlertsTab code={code} />}
        {activeTab === "settings" && <SettingsTab code={code} />}
      </div>
    </div>
  );
}

/**
 * Entry-point button for Retrospective Mode (ADAC-51 commit 8).
 *
 * Renders a clock icon next to the pacing badge. Click opens a small popover
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
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "flex h-7 items-center gap-1 rounded-md px-2 text-xs transition-colors",
          open
            ? "bg-amber-500/15 text-amber-300"
            : "text-slate-400 hover:bg-slate-800 hover:text-slate-200"
        )}
        aria-label="View historical snapshot"
        aria-expanded={open}
        title="View historical snapshot"
      >
        <Clock className="h-3.5 w-3.5" />
        <span className="hidden sm:inline">History</span>
      </button>
      {open && (
        <div className="absolute left-0 top-full z-30 mt-2 w-64 rounded-md border border-slate-700 bg-slate-900 p-3 shadow-lg">
          <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">
            View as of date
          </p>
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") submit();
            }}
            autoFocus
            className="mt-2 w-full rounded-md border border-slate-700 bg-slate-800 px-2 py-1.5 text-sm text-slate-200 focus:border-amber-400 focus:outline-none"
          />
          <div className="mt-3 flex items-center justify-end gap-2">
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded-md px-2 py-1 text-xs text-slate-400 hover:text-slate-200"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={submit}
              disabled={!date}
              className="rounded-md bg-amber-500/20 px-3 py-1 text-xs font-medium text-amber-200 hover:bg-amber-500/30 disabled:opacity-50"
            >
              View snapshot
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
