"use client";

import { useCallback, useEffect, useState } from "react";
import {
  Loader2,
  Plus,
  RefreshCw,
  Trash2,
  Undo2,
  Save,
  X,
} from "lucide-react";
import { api, type ProjectPlan, type SyncAllResponse } from "@/lib/api";
import { cn } from "@/lib/utils";

interface PlansSectionProps {
  projectCode: string;
  /** Notify the parent when a sync finishes so it can refresh the project row. */
  onChange?: () => void;
}

export function PlansSection({ projectCode, onChange }: PlansSectionProps) {
  const [plans, setPlans] = useState<ProjectPlan[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editingSheet, setEditingSheet] = useState<string | null>(null);
  const [editFields, setEditFields] = useState<{
    phase_label: string;
    display_order: string;
  }>({ phase_label: "", display_order: "" });
  const [adding, setAdding] = useState(false);
  const [addFields, setAddFields] = useState<{
    sheet_url_or_id: string;
    phase_label: string;
    display_order: string;
  }>({ sheet_url_or_id: "", phase_label: "", display_order: "" });
  const [syncSummary, setSyncSummary] = useState<SyncAllResponse | null>(null);

  const fetchPlans = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await api.admin.plans.list(projectCode);
      setPlans(r.plans);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load plans");
    } finally {
      setLoading(false);
    }
  }, [projectCode]);

  useEffect(() => {
    fetchPlans();
  }, [fetchPlans]);

  async function handleAdd() {
    if (!addFields.sheet_url_or_id.trim()) return;
    setActionLoading("add");
    try {
      const r = await api.admin.plans.add(projectCode, {
        sheet_url_or_id: addFields.sheet_url_or_id.trim(),
        phase_label: addFields.phase_label.trim() || null,
        display_order: addFields.display_order
          ? parseInt(addFields.display_order, 10)
          : null,
        auto_sync: true,
      });
      setPlans(r.plans);
      setAdding(false);
      setAddFields({ sheet_url_or_id: "", phase_label: "", display_order: "" });
      onChange?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add plan");
    } finally {
      setActionLoading(null);
    }
  }

  async function handleRetire(sheetId: string) {
    setActionLoading(`retire-${sheetId}`);
    try {
      const r = await api.admin.plans.remove(projectCode, sheetId);
      setPlans(r.plans);
      onChange?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to retire plan");
    } finally {
      setActionLoading(null);
    }
  }

  async function handleReactivate(sheetId: string) {
    setActionLoading(`reactivate-${sheetId}`);
    try {
      const r = await api.admin.plans.update(projectCode, sheetId, {
        is_active: true,
      });
      setPlans(r.plans);
      onChange?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to reactivate plan");
    } finally {
      setActionLoading(null);
    }
  }

  async function handleHardDelete(sheetId: string) {
    if (!confirm(`Permanently delete sheet ${sheetId.slice(0, 8)}…? This loses retrospective access.`)) return;
    setActionLoading(`delete-${sheetId}`);
    try {
      const r = await api.admin.plans.remove(projectCode, sheetId, true);
      setPlans(r.plans);
      onChange?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete plan");
    } finally {
      setActionLoading(null);
    }
  }

  function startEdit(p: ProjectPlan) {
    setEditingSheet(p.sheet_id);
    setEditFields({
      phase_label: p.phase_label ?? "",
      display_order: p.display_order != null ? String(p.display_order) : "",
    });
  }

  async function saveEdit(sheetId: string) {
    setActionLoading(`edit-${sheetId}`);
    try {
      const r = await api.admin.plans.update(projectCode, sheetId, {
        phase_label: editFields.phase_label.trim() || null,
        display_order: editFields.display_order
          ? parseInt(editFields.display_order, 10)
          : null,
      });
      setPlans(r.plans);
      setEditingSheet(null);
      onChange?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update plan");
    } finally {
      setActionLoading(null);
    }
  }

  async function handleSyncAll() {
    setActionLoading("sync-all");
    setSyncSummary(null);
    try {
      const r = await api.admin.plans.syncAll(projectCode);
      setSyncSummary(r);
      await fetchPlans();
      onChange?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to sync");
    } finally {
      setActionLoading(null);
    }
  }

  return (
    <div className="space-y-3 px-4 py-4 bg-slate-900/40 border-t border-slate-800">
      <div className="flex items-center justify-between">
        <h4 className="text-xs uppercase tracking-wider text-slate-500">
          Media Plan Sheets ({plans.filter((p) => p.is_active).length} active
          {plans.some((p) => !p.is_active) ? ` / ${plans.length} total` : ""})
        </h4>
        <div className="flex items-center gap-2">
          <button
            onClick={handleSyncAll}
            disabled={actionLoading === "sync-all" || plans.filter((p) => p.is_active).length === 0}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-700 px-3 py-1.5 text-xs text-slate-300 hover:text-white hover:bg-slate-800 transition-colors disabled:opacity-40"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", actionLoading === "sync-all" && "animate-spin")} />
            Sync all
          </button>
          <button
            onClick={() => setAdding((a) => !a)}
            className="inline-flex items-center gap-1.5 rounded-md bg-brand-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-brand-500"
          >
            <Plus className="h-3.5 w-3.5" />
            Add sheet
          </button>
        </div>
      </div>

      {adding && (
        <div className="rounded-md border border-slate-700 bg-slate-900 p-3 space-y-2">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            <input
              autoFocus
              value={addFields.sheet_url_or_id}
              onChange={(e) =>
                setAddFields((f) => ({ ...f, sheet_url_or_id: e.target.value }))
              }
              placeholder="Sheet URL or ID"
              className="rounded border border-slate-700 bg-slate-950 px-2 py-1.5 text-xs text-white md:col-span-1"
            />
            <input
              value={addFields.phase_label}
              onChange={(e) =>
                setAddFields((f) => ({ ...f, phase_label: e.target.value }))
              }
              placeholder='Phase label (e.g. "Pre-writ")'
              className="rounded border border-slate-700 bg-slate-950 px-2 py-1.5 text-xs text-white"
            />
            <input
              value={addFields.display_order}
              onChange={(e) =>
                setAddFields((f) => ({ ...f, display_order: e.target.value }))
              }
              placeholder="Order (1, 2, …)"
              type="number"
              className="rounded border border-slate-700 bg-slate-950 px-2 py-1.5 text-xs text-white"
            />
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleAdd}
              disabled={actionLoading === "add" || !addFields.sheet_url_or_id.trim()}
              className="inline-flex items-center gap-1.5 rounded-md bg-brand-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-brand-500 disabled:opacity-40"
            >
              {actionLoading === "add" ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Plus className="h-3.5 w-3.5" />
              )}
              Add &amp; sync
            </button>
            <button
              onClick={() => setAdding(false)}
              className="inline-flex items-center gap-1.5 rounded-md border border-slate-700 px-3 py-1.5 text-xs text-slate-400 hover:text-white"
            >
              <X className="h-3.5 w-3.5" />
              Cancel
            </button>
          </div>
        </div>
      )}

      {syncSummary && (
        <div
          className={cn(
            "rounded-md border px-3 py-2 text-xs",
            syncSummary.sheets_failed > 0
              ? "border-amber-500/30 bg-amber-500/10 text-amber-200"
              : "border-emerald-500/30 bg-emerald-500/10 text-emerald-200",
          )}
        >
          Synced {syncSummary.sheets_succeeded}/{syncSummary.sheets_attempted} sheets
          {syncSummary.sheets_failed > 0 && ` (${syncSummary.sheets_failed} failed)`}.
          {syncSummary.results
            .filter((r) => r.status !== "success")
            .map((r) => (
              <div key={r.sheet_id} className="mt-1 font-mono text-[11px]">
                {r.sheet_id.slice(0, 8)}…: {r.message}
              </div>
            ))}
        </div>
      )}

      {error && (
        <div className="rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-300">
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center py-4">
          <Loader2 className="h-4 w-4 animate-spin text-slate-500" />
        </div>
      ) : plans.length === 0 ? (
        <div className="text-xs text-slate-500 py-2">
          No media plan sheets registered. Add one to enable pacing &amp; diagnostics.
        </div>
      ) : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-left text-[10px] uppercase tracking-wider text-slate-500">
              <th className="px-2 py-1 w-12">#</th>
              <th className="px-2 py-1">Phase</th>
              <th className="px-2 py-1">Sheet ID</th>
              <th className="px-2 py-1 text-right">Lines</th>
              <th className="px-2 py-1">Last sync</th>
              <th className="px-2 py-1">Status</th>
              <th className="px-2 py-1 text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            {plans.map((p) => {
              const isEditing = editingSheet === p.sheet_id;
              return (
                <tr
                  key={p.sheet_id}
                  className={cn(
                    "border-t border-slate-800/50",
                    !p.is_active && "opacity-50",
                  )}
                >
                  <td className="px-2 py-1.5 text-slate-500">
                    {isEditing ? (
                      <input
                        value={editFields.display_order}
                        onChange={(e) =>
                          setEditFields((f) => ({ ...f, display_order: e.target.value }))
                        }
                        type="number"
                        className="w-12 rounded border border-slate-700 bg-slate-950 px-1 py-0.5 text-xs text-white"
                      />
                    ) : (
                      p.display_order ?? "—"
                    )}
                  </td>
                  <td className="px-2 py-1.5">
                    {isEditing ? (
                      <input
                        value={editFields.phase_label}
                        onChange={(e) =>
                          setEditFields((f) => ({ ...f, phase_label: e.target.value }))
                        }
                        placeholder="Phase label"
                        className="w-32 rounded border border-slate-700 bg-slate-950 px-1.5 py-0.5 text-xs text-white"
                      />
                    ) : (
                      <span className={cn(p.phase_label ? "text-white" : "text-slate-500")}>
                        {p.phase_label ?? "—"}
                      </span>
                    )}
                  </td>
                  <td className="px-2 py-1.5">
                    <a
                      href={`https://docs.google.com/spreadsheets/d/${p.sheet_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="font-mono text-[11px] text-brand-400 hover:underline"
                      title={p.sheet_id}
                    >
                      {p.sheet_id.slice(0, 12)}…
                    </a>
                  </td>
                  <td className="px-2 py-1.5 text-right tabular-nums text-slate-300">
                    {p.line_count > 0 ? p.line_count : <span className="text-slate-600">0</span>}
                  </td>
                  <td className="px-2 py-1.5 text-slate-400">
                    {p.last_synced_at ? p.last_synced_at.slice(0, 10) : <span className="text-slate-600">never</span>}
                  </td>
                  <td className="px-2 py-1.5">
                    {p.is_active ? (
                      <span className="rounded bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-medium text-emerald-400">
                        active
                      </span>
                    ) : (
                      <span className="rounded bg-slate-700/50 px-1.5 py-0.5 text-[10px] font-medium text-slate-400">
                        retired
                      </span>
                    )}
                  </td>
                  <td className="px-2 py-1.5">
                    <div className="flex justify-end gap-1">
                      {isEditing ? (
                        <>
                          <button
                            onClick={() => saveEdit(p.sheet_id)}
                            disabled={actionLoading === `edit-${p.sheet_id}`}
                            className="rounded bg-brand-600 p-1 text-white hover:bg-brand-500 disabled:opacity-40"
                            title="Save"
                          >
                            <Save className="h-3 w-3" />
                          </button>
                          <button
                            onClick={() => setEditingSheet(null)}
                            className="rounded border border-slate-700 p-1 text-slate-400 hover:text-white"
                            title="Cancel"
                          >
                            <X className="h-3 w-3" />
                          </button>
                        </>
                      ) : (
                        <>
                          <button
                            onClick={() => startEdit(p)}
                            className="rounded border border-slate-700 px-1.5 py-0.5 text-[10px] text-slate-400 hover:text-white"
                          >
                            Edit
                          </button>
                          {p.is_active ? (
                            <button
                              onClick={() => handleRetire(p.sheet_id)}
                              disabled={actionLoading === `retire-${p.sheet_id}`}
                              className="rounded border border-slate-700 p-1 text-amber-400 hover:bg-amber-500/10 disabled:opacity-40"
                              title="Retire (soft delete — preserves retrospective)"
                            >
                              <Trash2 className="h-3 w-3" />
                            </button>
                          ) : (
                            <>
                              <button
                                onClick={() => handleReactivate(p.sheet_id)}
                                disabled={actionLoading === `reactivate-${p.sheet_id}`}
                                className="rounded border border-slate-700 p-1 text-emerald-400 hover:bg-emerald-500/10 disabled:opacity-40"
                                title="Reactivate"
                              >
                                <Undo2 className="h-3 w-3" />
                              </button>
                              <button
                                onClick={() => handleHardDelete(p.sheet_id)}
                                disabled={actionLoading === `delete-${p.sheet_id}`}
                                className="rounded border border-slate-700 p-1 text-red-400 hover:bg-red-500/10 disabled:opacity-40"
                                title="Hard delete"
                              >
                                <Trash2 className="h-3 w-3" />
                              </button>
                            </>
                          )}
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}
