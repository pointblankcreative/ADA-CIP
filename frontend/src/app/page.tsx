"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  ArrowRight,
  Calendar,
  DollarSign,
  TrendingUp,
  AlertTriangle,
  RefreshCw,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { api, type Project } from "@/lib/api";
import { Card, KpiCard } from "@/components/card";
import { PacingBadge } from "@/components/pacing-badge";
import { BudgetGauge } from "@/components/budget-gauge";
import {
  formatCurrency,
  formatNumber,
  pacingStatus,
  pacingBg,
  cn,
} from "@/lib/utils";

export default function OverviewPage() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCompleted, setShowCompleted] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const data = await api.projects.list();
      setProjects(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load projects");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const activeProjects = projects.filter(
    (p) => p.status === "active" && !p.recently_ended
  );
  const recentlyEndedProjects = projects.filter(
    (p) => p.recently_ended === true
  );
  const completedProjects = projects.filter(
    (p) => p.status !== "active" && !p.recently_ended
  );
  const totalBudget = activeProjects.reduce((s, p) => s + (p.net_budget ?? 0), 0);
  const totalSpend = activeProjects.reduce((s, p) => s + (p.total_spend ?? 0), 0);

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      {/* Header */}
      <div className="flex items-center justify-between gap-3 pl-10 md:pl-0">
        <div>
          <h1 className="text-xl font-bold tracking-tight text-white sm:text-2xl">
            Campaign Overview
          </h1>
          <p className="mt-1 text-sm text-slate-500">
            All active campaigns with real-time pacing status
          </p>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-2 rounded-md border border-slate-700 bg-surface-raised px-3.5 py-2 text-sm text-slate-300 transition-colors hover:bg-slate-700 disabled:opacity-50"
        >
          <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
          Refresh
        </button>
      </div>

      {/* KPI strip */}
      <div className="mt-6 grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KpiCard
          label="Active Campaigns"
          value={String(activeProjects.length)}
          sub={`${projects.length} total`}
        />
        <KpiCard
          label="Total Budget"
          value={formatCurrency(totalBudget)}
          sub="across all projects"
        />
        <KpiCard
          label="Total Spend"
          value={formatCurrency(totalSpend)}
          accent={
            totalBudget > 0 && totalSpend / totalBudget > 0.9
              ? "text-amber-400"
              : "text-white"
          }
        />
        <KpiCard
          label="Spend Rate"
          value={
            totalBudget > 0
              ? `${((totalSpend / totalBudget) * 100).toFixed(1)}%`
              : "—"
          }
          sub="of total budget used"
        />
      </div>

      {/* Error */}
      {error && (
        <div className="mt-6 rounded-lg border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
          {error}
        </div>
      )}

      {/* Active project cards */}
      <div className="mt-8">
        <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
          Active Campaigns
        </h2>
        <div className="mt-3 grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
          {loading
            ? Array.from({ length: 3 }).map((_, i) => (
                <Card key={i} className="animate-pulse">
                  <div className="h-4 w-32 rounded bg-slate-700" />
                  <div className="mt-3 h-6 w-24 rounded bg-slate-700" />
                  <div className="mt-4 h-2 w-full rounded bg-slate-700" />
                </Card>
              ))
            : activeProjects.length === 0
              ? <p className="text-sm text-slate-500 col-span-full">No active campaigns.</p>
              : activeProjects.map((p) => (
                  <ProjectCard key={p.project_code} project={p} />
                ))}
        </div>
      </div>

      {/* Recently ended campaigns */}
      {!loading && recentlyEndedProjects.length > 0 && (
        <div className="mt-8">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-500">
            Recently Ended
          </h2>
          <div className="mt-3 grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
            {recentlyEndedProjects.map((p) => (
              <RecentlyEndedCard key={p.project_code} project={p} />
            ))}
          </div>
        </div>
      )}

      {/* Completed campaigns */}
      {!loading && completedProjects.length > 0 && (
        <div className="mt-8">
          <button
            onClick={() => setShowCompleted(!showCompleted)}
            className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-slate-500 hover:text-slate-300 transition-colors"
          >
            Completed Campaigns ({completedProjects.length})
            {showCompleted
              ? <ChevronUp className="h-4 w-4" />
              : <ChevronDown className="h-4 w-4" />}
          </button>
          {showCompleted && (
            <div className="mt-3 grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
              {completedProjects.map((p) => (
                <ProjectCard key={p.project_code} project={p} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function RecentlyEndedCard({ project: p }: { project: Project }) {
  return (
    <Link href={`/project/${p.project_code}`}>
      <Card className="group cursor-pointer opacity-60 transition-all hover:opacity-80 hover:border-slate-600">
        <div className="flex items-start justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-slate-400">
                {p.project_code}
              </span>
              <span className="rounded-full border border-slate-700 bg-slate-800/50 px-2 py-0.5 text-[10px] font-medium text-slate-500">
                Ended
              </span>
            </div>
            <h3 className="mt-2 truncate text-base font-semibold text-slate-300">
              {p.project_name}
            </h3>
            {p.client_name && (
              <p className="mt-0.5 text-xs text-slate-600">{p.client_name}</p>
            )}
          </div>
          <ArrowRight className="mt-1 h-4 w-4 flex-shrink-0 text-slate-700 transition-colors group-hover:text-slate-500" />
        </div>
        <div className="mt-3 flex items-center gap-4 text-xs text-slate-600">
          <span className="flex items-center gap-1">
            <Calendar className="h-3 w-3" />
            Ended {p.end_date}
          </span>
          <span className="flex items-center gap-1">
            <DollarSign className="h-3 w-3" />
            {formatCurrency(p.total_spend)} final spend
          </span>
        </div>
      </Card>
    </Link>
  );
}

function ProjectCard({ project: p }: { project: Project }) {
  const status = pacingStatus(p.pacing_percentage);
  const spendPct =
    p.net_budget > 0 ? (p.total_spend / p.net_budget) * 100 : 0;

  return (
    <Link href={`/project/${p.project_code}`}>
      <Card className="group cursor-pointer transition-colors hover:border-slate-600">
        <div className="flex items-start justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-slate-400">
                {p.project_code}
              </span>
              <PacingBadge
                percentage={p.pacing_percentage}
                totalSpend={p.total_spend}
                size="sm"
              />
            </div>
            <h3 className="mt-2 truncate text-base font-semibold text-white">
              {p.project_name}
            </h3>
            {p.client_name && (
              <p className="mt-0.5 text-xs text-slate-500">{p.client_name}</p>
            )}
          </div>
          <ArrowRight className="mt-1 h-4 w-4 flex-shrink-0 text-slate-600 transition-colors group-hover:text-slate-400" />
        </div>

        {/* Budget bar */}
        <div className="mt-4">
          <div className="flex items-baseline justify-between text-xs">
            <span className="text-slate-400">
              {formatCurrency(p.total_spend)} spent
            </span>
            <span className="text-slate-500">
              {formatCurrency(p.net_budget)}
            </span>
          </div>
          <BudgetGauge
            spent={p.total_spend}
            budget={p.net_budget}
            className="mt-1.5"
          />
        </div>

        {/* Footer */}
        <div className="mt-3 flex items-center gap-4 text-xs text-slate-500">
          <span className="flex items-center gap-1">
            <Calendar className="h-3 w-3" />
            {p.days_remaining > 0
              ? `${p.days_remaining}d left`
              : p.days_remaining === 0
              ? "Ends today"
              : "Ended"}
          </span>
          <span className="flex items-center gap-1">
            <DollarSign className="h-3 w-3" />
            {spendPct.toFixed(0)}% used
          </span>
        </div>
      </Card>
    </Link>
  );
}
