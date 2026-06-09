"use client";

import { Fragment, useEffect, useState, useCallback } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Loader2,
  Pencil,
  RotateCw,
  ChevronDown,
  ChevronRight,
} from "lucide-react";
import { api, type AdminProject } from "@/lib/api";
import { Card } from "@/components/card";
import { TH_CLS } from "@/lib/chart-theme";
import { formatCurrency, cn } from "@/lib/utils";
import { PlansSection } from "./plans-section";

const STATUS_STYLES: Record<string, string> = {
  active: "text-ok bg-tint-ok",
  planning: "text-info bg-tint-info",
  paused: "text-warn bg-tint-warn",
  completed: "text-done bg-tint-done",
};

const EDIT_INPUT_CLS =
  "rounded-sm border-2 border-line bg-surface-sunken px-2 py-1 text-xs text-fg outline-none focus:border-accent";

export default function ManageProjectsPage() {
  const [projects, setProjects] = useState<AdminProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [editCode, setEditCode] = useState<string | null>(null);
  const [editFields, setEditFields] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  // Which projects have their multi-plan section expanded.
  const [expandedPlans, setExpandedPlans] = useState<Set<string>>(new Set());

  const fetchProjects = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setProjects(await api.admin.projects.list());
    } catch (e) {
      console.error("Failed to fetch projects:", e);
      setError(e instanceof Error ? e.message : "Failed to load projects");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchProjects(); }, [fetchProjects]);

  async function handleResync(code: string, sheetId: string) {
    setActionLoading(`sync-${code}`);
    try {
      await api.admin.syncMediaPlan(sheetId, code);
      await fetchProjects();
    } catch { /* ignore */ } finally {
      setActionLoading(null);
    }
  }

  async function handleRunPacing(code: string) {
    setActionLoading(`pacing-${code}`);
    try {
      await api.admin.runPacing(code);
      await fetchProjects();
    } catch { /* ignore */ } finally {
      setActionLoading(null);
    }
  }

  function startEdit(p: AdminProject) {
    setEditCode(p.project_code);
    setEditFields({
      status: p.status,
      net_budget: String(p.net_budget ?? ""),
      slack_channel_id: p.slack_channel_id ?? "",
      media_plan_tab_name: p.media_plan_tab_name ?? "",
    });
  }

  async function saveEdit(code: string) {
    setActionLoading(`edit-${code}`);
    try {
      const payload: Record<string, unknown> = {};
      if (editFields.status) payload.status = editFields.status;
      if (editFields.net_budget) payload.net_budget = parseFloat(editFields.net_budget);
      if (editFields.slack_channel_id !== undefined) payload.slack_channel_id = editFields.slack_channel_id;
      if (editFields.media_plan_tab_name !== undefined) payload.media_plan_tab_name = editFields.media_plan_tab_name || null;
      await api.admin.projects.update(code, payload);
      setEditCode(null);
      await fetchProjects();
    } catch { /* ignore */ } finally {
      setActionLoading(null);
    }
  }

  function togglePlans(code: string) {
    setExpandedPlans((prev) => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code);
      else next.add(code);
      return next;
    });
  }

  return (
    <div className="mx-auto max-w-[1340px]">
      <div className="flex items-center justify-between">
        <div>
          <Link
            href="/admin"
            className="mb-3 inline-flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-[0.08em] text-fg-muted transition-colors hover:text-fg"
          >
            <ArrowLeft className="h-3.5 w-3.5" /> Admin
          </Link>
          <h1 className="text-xl font-extrabold tracking-tight text-fg">Manage Projects</h1>
        </div>
        <div className="flex gap-2">
          <Link
            href="/admin/projects/new"
            className="rounded-sm border-2 border-accent bg-accent px-4 py-2 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover"
          >
            + New Project
          </Link>
          <button
            onClick={fetchProjects}
            className="rounded-sm border-2 border-line px-3 py-2 text-fg-muted transition-colors hover:border-line-strong hover:text-fg"
          >
            <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
          </button>
        </div>
      </div>

      <Card className="mt-5 overflow-x-auto p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-line-soft text-left">
              <th className={TH_CLS}>Code</th>
              <th className={TH_CLS}>Name</th>
              <th className={TH_CLS}>Client</th>
              <th className={TH_CLS}>Status</th>
              <th className={cn(TH_CLS, "text-right")}>Budget</th>
              <th className={cn(TH_CLS, "text-center")}>Sheet</th>
              <th className={TH_CLS}>Tab</th>
              <th className={TH_CLS}>Slack</th>
              <th className={cn(TH_CLS, "text-center")}>Alerts</th>
              <th className={TH_CLS}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={10} className="px-4 py-12 text-center text-fg-muted">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                </td>
              </tr>
            )}
            {!loading && error && (
              <tr>
                <td colSpan={10} className="px-4 py-12 text-center text-danger">
                  Error loading projects: {error}
                </td>
              </tr>
            )}
            {!loading && !error && projects.length === 0 && (
              <tr>
                <td colSpan={10} className="px-4 py-12 text-center text-fg-muted">
                  No projects found.
                </td>
              </tr>
            )}
            {projects.map((p) => {
              const isEditing = editCode === p.project_code;
              const plansOpen = expandedPlans.has(p.project_code);
              return (
                <Fragment key={p.project_code}>
                <tr className="border-b border-line-soft transition-colors hover:bg-surface-sunken">
                  <td className="px-4 py-3">
                    <Link href={`/project/${p.project_code}`} className="font-mono text-accent-ink hover:underline">
                      {p.project_code}
                    </Link>
                  </td>
                  <td className="max-w-[200px] truncate px-4 py-3 font-medium text-fg">{p.project_name}</td>
                  <td className="px-4 py-3 text-fg-muted">{p.client_name ?? "—"}</td>
                  <td className="px-4 py-3">
                    {isEditing ? (
                      <select
                        value={editFields.status}
                        onChange={(e) => setEditFields((f) => ({ ...f, status: e.target.value }))}
                        className={EDIT_INPUT_CLS}
                      >
                        {["planning", "active", "paused", "completed"].map((s) => (
                          <option key={s} value={s}>{s}</option>
                        ))}
                      </select>
                    ) : (
                      <span className={cn("rounded-pill px-2 py-0.5 font-mono text-[10.5px] font-medium uppercase tracking-[0.06em]", STATUS_STYLES[p.status] ?? STATUS_STYLES.active)}>
                        {p.status}
                      </span>
                    )}
                  </td>
                  <td className="tnum px-4 py-3 text-right font-mono text-fg">
                    {isEditing ? (
                      <input
                        type="number"
                        value={editFields.net_budget}
                        onChange={(e) => setEditFields((f) => ({ ...f, net_budget: e.target.value }))}
                        className={cn(EDIT_INPUT_CLS, "w-28 text-right")}
                      />
                    ) : (
                      formatCurrency(p.net_budget)
                    )}
                  </td>
                  <td className="px-4 py-3 text-center">
                    <button
                      onClick={() => togglePlans(p.project_code)}
                      className="inline-flex items-center gap-1 rounded-sm p-1 text-fg-muted transition-colors hover:bg-surface-sunken hover:text-fg"
                      title="Manage media plan sheets"
                    >
                      {plansOpen ? (
                        <ChevronDown className="h-3.5 w-3.5" />
                      ) : (
                        <ChevronRight className="h-3.5 w-3.5" />
                      )}
                      {p.media_plan_synced ? (
                        <CheckCircle2 className="h-4 w-4 text-ok" />
                      ) : (
                        <XCircle className="h-4 w-4 text-fg-faint" />
                      )}
                    </button>
                  </td>
                  <td className="px-4 py-3 text-xs text-fg-muted">
                    {isEditing ? (
                      <input
                        value={editFields.media_plan_tab_name}
                        onChange={(e) => setEditFields((f) => ({ ...f, media_plan_tab_name: e.target.value }))}
                        className={cn(EDIT_INPUT_CLS, "w-32")}
                        placeholder="Tab name"
                      />
                    ) : (
                      p.media_plan_tab_name || "—"
                    )}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-fg-muted">
                    {isEditing ? (
                      <input
                        value={editFields.slack_channel_id}
                        onChange={(e) => setEditFields((f) => ({ ...f, slack_channel_id: e.target.value }))}
                        className={cn(EDIT_INPUT_CLS, "w-32")}
                        placeholder="#channel"
                      />
                    ) : (
                      p.slack_channel_id || "—"
                    )}
                  </td>
                  <td className="px-4 py-3 text-center">
                    {p.alert_count > 0 ? (
                      <span className="rounded-pill bg-tint-danger px-2 py-0.5 font-mono text-xs font-medium text-danger">
                        {p.alert_count}
                      </span>
                    ) : (
                      <span className="text-fg-faint">0</span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1">
                      {isEditing ? (
                        <>
                          <button
                            onClick={() => saveEdit(p.project_code)}
                            disabled={actionLoading === `edit-${p.project_code}`}
                            className="rounded-sm border-2 border-accent bg-accent px-2 py-1 text-xs font-bold text-on-accent hover:bg-accent-hover"
                          >
                            {actionLoading === `edit-${p.project_code}` ? "..." : "Save"}
                          </button>
                          <button
                            onClick={() => setEditCode(null)}
                            className="rounded-sm border-2 border-line px-2 py-1 text-xs text-fg-muted hover:text-fg"
                          >
                            Cancel
                          </button>
                        </>
                      ) : (
                        <>
                          <button
                            onClick={() => startEdit(p)}
                            className="rounded-sm border-2 border-line p-1.5 text-fg-muted transition-colors hover:border-line-strong hover:text-fg"
                            title="Edit"
                          >
                            <Pencil className="h-3.5 w-3.5" />
                          </button>
                          {p.media_plan_sheet_id && (
                            <button
                              onClick={() => handleResync(p.project_code, p.media_plan_sheet_id!)}
                              disabled={actionLoading === `sync-${p.project_code}`}
                              className="rounded-sm border-2 border-line p-1.5 text-fg-muted transition-colors hover:border-line-strong hover:text-fg disabled:opacity-50"
                              title="Re-sync media plan"
                            >
                              <RefreshCw className={cn("h-3.5 w-3.5", actionLoading === `sync-${p.project_code}` && "animate-spin")} />
                            </button>
                          )}
                          <button
                            onClick={() => handleRunPacing(p.project_code)}
                            disabled={actionLoading === `pacing-${p.project_code}`}
                            className="rounded-sm border-2 border-line p-1.5 text-fg-muted transition-colors hover:border-line-strong hover:text-fg disabled:opacity-50"
                            title="Re-run pacing"
                          >
                            <RotateCw className={cn("h-3.5 w-3.5", actionLoading === `pacing-${p.project_code}` && "animate-spin")} />
                          </button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
                {plansOpen && (
                  <tr className="bg-surface-sunken">
                    <td colSpan={10} className="p-0">
                      <PlansSection
                        projectCode={p.project_code}
                        onChange={fetchProjects}
                      />
                    </td>
                  </tr>
                )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
