"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import {
  ArrowLeft,
  Gauge,
  BarChart3,
  AlertTriangle,
  Settings2,
} from "lucide-react";
import { api, type Project } from "@/lib/api";
import { PacingBadge } from "@/components/pacing-badge";
import { cn, formatCurrency } from "@/lib/utils";
import { PacingTab } from "./pacing-tab";
import { PerformanceTab } from "./performance-tab";
import { AlertsTab } from "./alerts-tab";
import { SettingsTab } from "./settings-tab";

const TABS = [
  { id: "pacing", label: "Pacing", icon: Gauge },
  { id: "performance", label: "Performance", icon: BarChart3 },
  { id: "alerts", label: "Alerts", icon: AlertTriangle },
  { id: "settings", label: "Settings", icon: Settings2 },
] as const;

type TabId = (typeof TABS)[number]["id"];

function isUnprovisioned(p: Project): boolean {
  return !p.net_budget || p.net_budget === 0;
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
      <header className="border-b border-slate-800 bg-surface px-6 py-4 lg:px-8">
        <div className="flex items-center gap-3">
          <Link
            href="/"
            className="flex h-7 w-7 items-center justify-center rounded-md text-slate-500 transition-colors hover:bg-slate-800 hover:text-slate-300"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-3">
              <span className="rounded bg-slate-800 px-2 py-0.5 font-mono text-xs text-slate-400">
                {code}
              </span>
              {loading ? (
                <div className="h-5 w-48 animate-pulse rounded bg-slate-700" />
              ) : (
                <h1 className="truncate text-lg font-semibold text-white">
                  {project?.project_name || `Project ${code}`}
                </h1>
              )}
              {project && !unprovisioned && (
                <PacingBadge percentage={project.pacing_percentage} totalSpend={project.total_spend} size="sm" />
              )}
            </div>
            {project && !unprovisioned && (
              <div className="mt-0.5 flex items-center gap-4 text-xs text-slate-500">
                <span>Budget: {formatCurrency(project.net_budget)}</span>
                <span>Spent: {formatCurrency(project.total_spend)}</span>
                <span>
                  {project.days_remaining > 0
                    ? `${project.days_remaining} days remaining`
                    : "Campaign ended"}
                </span>
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
        <nav className="mt-4 flex gap-1">
          {TABS.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={cn(
                "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm transition-colors",
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
      <div className="flex-1 p-6 lg:p-8">
        {activeTab === "pacing" && <PacingTab code={code} />}
        {activeTab === "performance" && <PerformanceTab code={code} />}
        {activeTab === "alerts" && <AlertsTab code={code} />}
        {activeTab === "settings" && <SettingsTab code={code} />}
      </div>
    </div>
  );
}
