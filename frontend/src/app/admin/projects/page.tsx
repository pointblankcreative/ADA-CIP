"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Loader2,
  Pencil,
  RotateCw,
} from "lucide-react";
import { api, type AdminProject } from "@/lib/api";
import { Card } from "@/components/card";
import { formatCurrency, cn } from "@/lib/utils";

const STATUS_STYLES: Record<string, string> = {
  active: "text-emerald-400 bg-emerald-500/15",
  planning: "text-blue-400 bg-blue-500/15",
  paused: "text-amber-400 bg-amber-500/15",
  completed: "text-slate-400 bg-slate-500/15",
};

export default function ManageProjectsPage() {
  const [projects, setProjects] = useState<AdminProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [editCode, setEditCode] = useState<string | null>(null);
  const [editFields, setEditFields] = useState<Record<string, string>>({});

  const fetchProjects = useCallback(async () => {
    setLoading(true);
    try {
      setProjects(await api.admin.projects.list());
    } catch { /* ignore */ } finally {
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
    });
  }

  async function saveEdit(code: string) {
    setActionLoading(`edit-${code}`);
    try {
      const payload: Record<string, unknown> = {};
      if (editFields.status) payload.status = editFields.status;
      if (editFields.net_budget) payload.net_budget = parseFloat(editFields.net_budget);
      if (editFields.slack_channel_id !== undefined) payload.slack_channel_id = editFields.slack_channel_id;
      await api.admin.projects.update(code, payload);
      setEditCode(null);
      await fetchProjects();
    } catch { /* ignore */ } finally {
      setActionLoading(null);
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between">
        <div>
          <Link
            href="/admin"
            className="inline-flex items-center gap-1.5 text-sm text-slate-400 hover:text-white transition-colors mb-3"
          >
            <ArrowLeft className="h-4 w-4" /> Admin
          </Link>
          <h1 className="text-xl font-semibold text-white">Manage Projects</h1>
        </div>
        <div className="flex gap-2">
          <Link
            href="/admin/projects/new"
            className="rounded-md bg-brand-600 px-4 py-2 text-sm font-medium text-white hover:bg-brand-500 transition-colors"
          >
            + New Project
          </Link>
          <button
            onClick={fetchProjects}
            className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-400 hover:text-white transition-colors"
          >
            <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
          </button>
        </div>
      </div>

      <Card className="mt-5 overflow-x-auto p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-800 text-left text-xs text-slate-500 uppercase tracking-wider">
              <th className="px-4 py-3">Code</th>
              <th className="px-4 py-3">Name</th>
              <th className="px-4 py-3">Client</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3 text-right">Budget</th>
              <th className="px-4 py-3 text-center">Sheet</th>
              <th className="px-4 py-3">Slack</th>
              <th className="px-4 py-3 text-center">Alerts</th>
              <th className="px-4 py-3">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={9} className="px-4 py-12 text-center text-slate-500">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                </td>
              </tr>
            )}
            {!loading && projects.length === 0 && (
              <tr>
                <td colSpan={9} className="px-4 py-12 text-center text-slate-500">
                  No projects found.
                </td>
              </tr>
            )}
            {projects.map((p) => {
              const isEditing = editCode === p.project_code;
              return (
                <tr
                  key={p.project_code}
                  className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors"
                >
                  <td className="px-4 py-3">
                    <Link href={`/project/${p.project_code}`} className="font-mono text-brand-400 hover:underline">
                      {p.project_code}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-white max-w-[200px] truncate">{p.project_name}</td>
                  <td className="px-4 py-3 text-slate-400">{p.client_name ?? "—"}</td>
                  <td className="px-4 py-3">
                    {isEditing ? (
                      <select
                        value={editFields.status}
                        onChange={(e) => setEditFields((f) => ({ ...f, status: e.target.value }))}
                        className="rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-white"
                      >
                        {["planning", "active", "paused", "completed"].map((s) => (
                          <option key={s} value={s}>{s}</option>
                        ))}
                      </select>
                    ) : (
                      <span className={cn("rounded px-2 py-0.5 text-xs font-medium", STATUS_STYLES[p.status] ?? STATUS_STYLES.active)}>
                        {p.status}
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-white">
                    {isEditing ? (
                      <input
                        type="number"
                        value={editFields.net_budget}
                        onChange={(e) => setEditFields((f) => ({ ...f, net_budget: e.target.value }))}
                        className="w-28 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-right text-white"
                      />
                    ) : (
                      formatCurrency(p.net_budget)
                    )}
                  </td>
                  <td className="px-4 py-3 text-center">
                    {p.media_plan_synced ? (
                      <CheckCircle2 className="mx-auto h-4 w-4 text-emerald-400" />
                    ) : (
                      <XCircle className="mx-auto h-4 w-4 text-slate-600" />
                    )}
                  </td>
                  <td className="px-4 py-3 text-xs text-slate-400">
                    {isEditing ? (
                      <input
                        value={editFields.slack_channel_id}
                        onChange={(e) => setEditFields((f) => ({ ...f, slack_channel_id: e.target.value }))}
                        className="w-32 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-white"
                        placeholder="#channel"
                      />
                    ) : (
                      p.slack_channel_id || "—"
                    )}
                  </td>
                  <td className="px-4 py-3 text-center">
                    {p.alert_count > 0 ? (
                      <span className="rounded bg-red-500/15 px-2 py-0.5 text-xs font-medium text-red-400">
                        {p.alert_count}
                      </span>
                    ) : (
                      <span className="text-slate-600">0</span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1">
                      {isEditing ? (
                        <>
                          <button
                            onClick={() => saveEdit(p.project_code)}
                            disabled={actionLoading === `edit-${p.project_code}`}
                            className="rounded bg-brand-600 px-2 py-1 text-xs text-white hover:bg-brand-500"
                          >
                            {actionLoading === `edit-${p.project_code}` ? "..." : "Save"}
                          </button>
                          <button
                            onClick={() => setEditCode(null)}
                            className="rounded border border-slate-700 px-2 py-1 text-xs text-slate-400 hover:text-white"
                          >
                            Cancel
                          </button>
                        </>
                      ) : (
                        <>
                          <button
                            onClick={() => startEdit(p)}
                            className="rounded border border-slate-700 p-1.5 text-slate-400 hover:text-white transition-colors"
                            title="Edit"
                          >
                            <Pencil className="h-3.5 w-3.5" />
                          </button>
                          {p.media_plan_sheet_id && (
                            <button
                              onClick={() => handleResync(p.project_code, p.media_plan_sheet_id!)}
                              disabled={actionLoading === `sync-${p.project_code}`}
                              className="rounded border border-slate-700 p-1.5 text-slate-400 hover:text-white transition-colors disabled:opacity-50"
                              title="Re-sync media plan"
                            >
                              <RefreshCw className={cn("h-3.5 w-3.5", actionLoading === `sync-${p.project_code}` && "animate-spin")} />
                            </button>
                          )}
                          <button
                            onClick={() => handleRunPacing(p.project_code)}
                            disabled={actionLoading === `pacing-${p.project_code}`}
                            className="rounded border border-slate-700 p-1.5 text-slate-400 hover:text-white transition-colors disabled:opacity-50"
                            title="Re-run pacing"
                          >
                            <RotateCw className={cn("h-3.5 w-3.5", actionLoading === `pacing-${p.project_code}` && "animate-spin")} />
                          </button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
