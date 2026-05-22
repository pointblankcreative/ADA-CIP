"use client";

/**
 * Retrospective Mode page (ADAC-51 commit 7).
 *
 * URL: /project/[code]/retrospective/[date]
 *
 * Renders pacing + diagnostics pinned to a past date. Reuses the live
 * page's PacingTab and DiagnosticsTab components with their new optional
 * asOfDate prop (commit 6). Layout differs from the live page in two
 * deliberate ways:
 *
 *   1. Amber banner at the top makes it visually obvious the user is
 *      looking at historical data, with a "Back to live" link.
 *   2. Date picker in the header lets the user navigate to other historical
 *      dates without hand-typing URLs. Picking a date pushes a new history
 *      entry so the back button does what you'd expect.
 *
 * Performance / Alerts / Settings tabs are deferred per the MVP scope
 * agreed on the build-plan questions: pacing + diagnostics is the
 * Retrospective Mode story for now.
 */

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { ArrowLeft, Clock, ChevronLeft, ChevronRight } from "lucide-react";
import {
  api,
  type Project,
} from "@/lib/api";
import { PacingTab } from "../../pacing-tab";
import {
  DiagnosticsTab,
  type RetrospectiveMetadata,
} from "../../diagnostics-tab";
import { cn, formatCurrency } from "@/lib/utils";

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function isValidIsoDate(value: string): boolean {
  if (!ISO_DATE_RE.test(value)) return false;
  const d = new Date(value + "T00:00:00");
  return !Number.isNaN(d.getTime());
}

function shiftDate(iso: string, deltaDays: number): string {
  const d = new Date(iso + "T00:00:00");
  d.setDate(d.getDate() + deltaDays);
  return d.toISOString().slice(0, 10);
}

function formatLongDate(iso: string): string {
  const d = new Date(iso + "T00:00:00");
  return d.toLocaleDateString("en-US", {
    weekday: "long",
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

export default function RetrospectivePage() {
  const params = useParams<{ code: string; date: string }>();
  const router = useRouter();
  const code = params.code;
  const date = params.date;

  const [project, setProject] = useState<Project | null>(null);
  const [loadingProject, setLoadingProject] = useState(true);
  const [meta, setMeta] = useState<RetrospectiveMetadata | null>(null);

  useEffect(() => {
    setLoadingProject(true);
    api.projects
      .get(code)
      .then(setProject)
      .catch(() => setProject(null))
      .finally(() => setLoadingProject(false));
  }, [code]);

  // Reset retro metadata when the date changes — DiagnosticsTab will
  // re-fire onRetrospectiveMetadata after its fetch settles.
  useEffect(() => {
    setMeta(null);
  }, [date]);

  const dateValid = isValidIsoDate(date);

  const goToDate = (newIso: string) => {
    if (!isValidIsoDate(newIso)) return;
    router.push(`/project/${code}/retrospective/${newIso}`);
  };

  return (
    <div className="flex min-h-screen flex-col">
      <header className="border-b border-slate-800 bg-surface px-4 py-4 sm:px-6 lg:px-8">
        {/* Top row: back-to-live + project label */}
        <div className="flex items-center gap-3 pl-10 md:pl-0">
          <Link
            href={`/project/${code}`}
            className="flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-md text-slate-500 transition-colors hover:bg-slate-800 hover:text-slate-300"
            aria-label="Back to live view"
            title="Back to live view"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2 sm:gap-3">
              <span className="rounded bg-slate-800 px-2 py-0.5 font-mono text-xs text-slate-400">
                {code}
              </span>
              {loadingProject ? (
                <div className="h-5 w-48 animate-pulse rounded bg-slate-700" />
              ) : (
                <h1 className="truncate text-base font-semibold text-white sm:text-lg">
                  {project?.project_name || `Project ${code}`}
                </h1>
              )}
              <span className="rounded bg-amber-500/15 px-2 py-0.5 text-[11px] font-medium uppercase tracking-wider text-amber-300">
                Historical
              </span>
            </div>
            {project && (
              <div className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-0.5 text-xs text-slate-500">
                <span>Budget: {formatCurrency(project.net_budget)} {project.currency}</span>
                <span>
                  Flight: {project.start_date} to {project.end_date}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* Banner: viewing as of DATE */}
        <div className="mt-4 flex flex-col gap-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-5 py-3.5 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-3">
            <Clock className="h-5 w-5 flex-shrink-0 text-amber-400" />
            <div>
              <p className="text-sm font-medium text-amber-300">
                {dateValid ? (
                  <>Viewing as of {formatLongDate(date)}</>
                ) : (
                  <>Invalid date in URL</>
                )}
              </p>
              <p className="mt-0.5 text-xs text-amber-400/80">
                {dateValid ? (
                  <>
                    Pacing and diagnostics shown reflect this project&rsquo;s
                    state on that day. Plan and FFS configuration shown
                    reflect today&rsquo;s values. Retired phases are
                    re-included so historical pacing roll-ups are complete.
                    {meta && (
                      <>
                        {" · "}
                        {meta.cached
                          ? "From snapshot cache"
                          : "Just computed"}
                        {" · engine "}
                        <span className="font-mono">
                          {meta.engineVersion.slice(0, 8)}
                        </span>
                      </>
                    )}
                  </>
                ) : (
                  <>Date must be in YYYY-MM-DD format. Pick a valid date below.</>
                )}
              </p>
            </div>
          </div>

          {/* Date picker + day-step buttons */}
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => dateValid && goToDate(shiftDate(date, -1))}
              disabled={!dateValid}
              className={cn(
                "flex h-8 w-8 items-center justify-center rounded-md border border-amber-500/30 text-amber-300 transition-colors",
                dateValid
                  ? "hover:bg-amber-500/20"
                  : "cursor-not-allowed opacity-50"
              )}
              aria-label="Previous day"
              title="Previous day"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            <input
              type="date"
              value={dateValid ? date : ""}
              onChange={(e) => goToDate(e.target.value)}
              className="rounded-md border border-amber-500/30 bg-slate-900/50 px-2 py-1 text-sm text-amber-200 focus:border-amber-400 focus:outline-none"
            />
            <button
              type="button"
              onClick={() => dateValid && goToDate(shiftDate(date, 1))}
              disabled={!dateValid}
              className={cn(
                "flex h-8 w-8 items-center justify-center rounded-md border border-amber-500/30 text-amber-300 transition-colors",
                dateValid
                  ? "hover:bg-amber-500/20"
                  : "cursor-not-allowed opacity-50"
              )}
              aria-label="Next day"
              title="Next day"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
            <Link
              href={`/project/${code}`}
              className="ml-2 rounded-md border border-amber-500/30 px-3 py-1 text-xs font-medium text-amber-300 hover:bg-amber-500/20 transition-colors"
            >
              Back to live
            </Link>
          </div>
        </div>
      </header>

      {/* Body: pacing + diagnostics, vertically stacked. Same components as
          the live page, just driven by asOfDate. */}
      <div className="flex-1 space-y-8 p-4 sm:p-6 lg:p-8">
        {dateValid ? (
          <>
            <section>
              <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-slate-500">
                Pacing
              </h2>
              <PacingTab code={code} asOfDate={date} />
            </section>
            <section>
              <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-slate-500">
                Diagnostics
              </h2>
              <DiagnosticsTab
                code={code}
                asOfDate={date}
                onRetrospectiveMetadata={setMeta}
              />
            </section>
          </>
        ) : (
          <div className="rounded-md border border-slate-800 bg-slate-900/40 p-6 text-sm text-slate-400">
            <p>The date <span className="font-mono">{date}</span> is not a valid YYYY-MM-DD.</p>
            <p className="mt-2">Pick a valid date in the picker above, or go{" "}
              <Link href={`/project/${code}`} className="text-amber-300 hover:underline">
                back to live
              </Link>.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
