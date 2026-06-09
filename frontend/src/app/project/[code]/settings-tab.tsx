"use client";

import { useEffect, useState, useCallback } from "react";
import { Plus, Trash2, Globe, Loader2, FileText, Pencil } from "lucide-react";
import {
  api,
  type GA4Url,
  type GA4Property,
  type FFSEntry,
  type PacingLine,
} from "@/lib/api";
import { Card } from "@/components/card";
import { FFSWizard } from "@/components/ffs-wizard";
import { ffsBucket } from "@/lib/ffs";
import { cn } from "@/lib/utils";

const INPUT_CLS =
  "w-full rounded-sm border-2 border-line bg-surface-sunken px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none focus:border-accent";

export function SettingsTab({ code }: { code: string }) {
  const [urls, setUrls] = useState<GA4Url[]>([]);
  const [properties, setProperties] = useState<GA4Property[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ ga4_property_id: "", url_pattern: "", label: "" });

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [u, p] = await Promise.all([
        api.ga4.urls(code),
        api.ga4.properties(),
      ]);
      setUrls(u);
      setProperties(p);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load GA4 configuration");
    } finally {
      setLoading(false);
    }
  }, [code]);

  useEffect(() => { loadData(); }, [loadData]);

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault();
    if (!form.ga4_property_id || !form.url_pattern) return;
    setAdding(true);
    setError(null);
    try {
      const url = await api.ga4.addUrl(code, {
        ga4_property_id: form.ga4_property_id,
        url_pattern: form.url_pattern,
        label: form.label || undefined,
      });
      setUrls((prev) => [...prev, url]);
      setForm({ ga4_property_id: form.ga4_property_id, url_pattern: "", label: "" });
      setShowForm(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save GA4 URL. Please try again.");
    } finally {
      setAdding(false);
    }
  }

  async function handleDelete(urlId: string) {
    setError(null);
    try {
      await api.ga4.deleteUrl(code, urlId);
      setUrls((prev) => prev.filter((u) => u.id !== urlId));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete GA4 URL");
    }
  }

  if (loading) {
    return (
      <Card className="animate-pulse">
        <div className="h-4 w-32 rounded bg-surface-sunken" />
        <div className="mt-4 h-24 rounded bg-surface-sunken" />
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      {error && (
        <div className="rounded-md border-2 border-tint-danger bg-tint-danger px-4 py-3 text-sm text-danger">
          {error}
        </div>
      )}

      <Card>
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-bold text-fg">GA4 Web Analytics URLs</h3>
            <p className="mt-1 text-xs text-fg-muted">
              Configure landing page URLs to pull GA4 web analytics data into the performance view.
            </p>
          </div>
          {!showForm && (
            <button
              onClick={() => { setShowForm(true); setError(null); }}
              className="flex items-center gap-1.5 rounded-sm border-2 border-line px-3 py-1.5 text-xs font-bold text-fg transition-colors hover:border-line-strong"
            >
              <Plus className="h-3.5 w-3.5" /> Add URL
            </button>
          )}
        </div>

        {/* Existing URLs */}
        {urls.length > 0 && (
          <div className="mt-4 space-y-2">
            {urls.map((u) => (
              <div
                key={u.id}
                className="flex items-center gap-3 rounded-sm border-2 border-line-soft bg-surface-sunken px-3 py-2.5"
              >
                <Globe className="h-4 w-4 flex-shrink-0 text-fg-faint" />
                <div className="min-w-0 flex-1">
                  <p className="truncate font-mono text-[13px] text-fg">{u.url_pattern}</p>
                  <p className="text-xs text-fg-muted">
                    GA4: {u.ga4_property_id}
                    {u.label && ` · ${u.label}`}
                  </p>
                </div>
                <button
                  onClick={() => handleDelete(u.id)}
                  className="flex-shrink-0 rounded-sm p-1 text-fg-faint transition-colors hover:bg-tint-danger hover:text-danger"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              </div>
            ))}
          </div>
        )}

        {urls.length === 0 && !showForm && (
          <div className="mt-4 rounded-sm border-2 border-dashed border-line px-4 py-6 text-center">
            <Globe className="mx-auto h-6 w-6 text-fg-faint" />
            <p className="mt-2 text-sm text-fg-muted">No GA4 URLs configured</p>
            <p className="mt-1 text-xs text-fg-faint">
              Add landing page URLs to see web analytics data on the performance tab.
            </p>
          </div>
        )}

        {/* Add URL form */}
        {showForm && (
          <form onSubmit={handleAdd} className="mt-4 space-y-3 rounded-sm border-2 border-line-soft bg-surface-sunken p-4">
            <div>
              <label className="label mb-1 block text-[10px]">
                GA4 Property
              </label>
              {properties.length > 0 ? (
                <select
                  value={form.ga4_property_id}
                  onChange={(e) => setForm((f) => ({ ...f, ga4_property_id: e.target.value }))}
                  className={INPUT_CLS}
                >
                  <option value="">Select property...</option>
                  {properties.map((p) => (
                    <option key={p.property_id} value={p.property_id}>
                      {p.property_name || p.property_id}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  placeholder="e.g. 123456789"
                  value={form.ga4_property_id}
                  onChange={(e) => setForm((f) => ({ ...f, ga4_property_id: e.target.value }))}
                  className={INPUT_CLS}
                />
              )}
            </div>
            <div>
              <label className="label mb-1 block text-[10px]">
                URL Pattern
              </label>
              <input
                required
                placeholder="e.g. example.com/campaign-landing-page"
                value={form.url_pattern}
                onChange={(e) => setForm((f) => ({ ...f, url_pattern: e.target.value }))}
                className={cn(INPUT_CLS, "font-mono text-[13px]")}
              />
              <p className="mt-1 text-xs text-fg-faint">
                Partial URL match — e.g. &quot;bcgeu.ca/bargaining&quot; matches all pages containing this path.
              </p>
            </div>
            <div>
              <label className="label mb-1 block text-[10px]">
                Label <span className="normal-case text-fg-faint">(optional)</span>
              </label>
              <input
                placeholder="e.g. Main landing page"
                value={form.label}
                onChange={(e) => setForm((f) => ({ ...f, label: e.target.value }))}
                className={INPUT_CLS}
              />
            </div>
            <div className="flex gap-2 pt-1">
              <button
                type="submit"
                disabled={adding || !form.ga4_property_id || !form.url_pattern}
                className="flex items-center gap-1.5 rounded-sm border-2 border-accent bg-accent px-4 py-2 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover disabled:opacity-50"
              >
                {adding && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                Add URL
              </button>
              <button
                type="button"
                onClick={() => setShowForm(false)}
                className="rounded-sm border-2 border-line px-4 py-2 text-sm font-bold text-fg transition-colors hover:border-line-strong"
              >
                Cancel
              </button>
            </div>
          </form>
        )}
      </Card>

      <FFSSection code={code} />
    </div>
  );
}

// ── FFS Section ─────────────────────────────────────────────────────────

function FFSSection({ code }: { code: string }) {
  const [entries, setEntries] = useState<FFSEntry[]>([]);
  const [lines, setLines] = useState<PacingLine[]>([]);
  const [lineLinks, setLineLinks] = useState<Record<string, string | null>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [wizardOpen, setWizardOpen] = useState(false);
  const [editingEntry, setEditingEntry] = useState<FFSEntry | null>(null);

  const loadFFS = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // Entries + pacing (for lines) in parallel.
      const [ents, pacing] = await Promise.all([
        api.ffs.list(code),
        api.pacing.get(code).catch(() => null),
      ]);
      setEntries(ents);
      setLines(pacing?.lines ?? []);
      // Build line_id → entry_id map so the wizard can flag lines already
      // linked to a different entry. Last-write wins if the same line is
      // linked to multiple entries (shouldn't happen — media_plan_lines has
      // a single ffs_entry_id column — but be defensive).
      const links: Record<string, string | null> = {};
      for (const ent of ents) {
        for (const lineId of ent.linked_line_ids) {
          links[lineId] = ent.entry_id;
        }
      }
      setLineLinks(links);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load FFS entries");
    } finally {
      setLoading(false);
    }
  }, [code]);

  useEffect(() => { loadFFS(); }, [loadFFS]);

  async function handleSubmit(payload: {
    label: string | null;
    lp_url: string | null;
    is_platform_form: boolean;
    platform_id: string | null;
    ffs_inputs: FFSEntry["ffs_inputs"];
    applied_line_ids: string[];
  }) {
    if (editingEntry) {
      await api.ffs.update(code, editingEntry.entry_id, {
        label: payload.label,
        lp_url: payload.lp_url,
        is_platform_form: payload.is_platform_form,
        platform_id: payload.platform_id,
        ffs_inputs: payload.ffs_inputs,
      });
      // Re-apply line mapping separately (update doesn't touch links).
      await api.ffs.apply(code, editingEntry.entry_id, payload.applied_line_ids);
    } else {
      await api.ffs.create(code, {
        label: payload.label,
        lp_url: payload.lp_url,
        is_platform_form: payload.is_platform_form,
        platform_id: payload.platform_id,
        ffs_inputs: payload.ffs_inputs,
        applied_line_ids: payload.applied_line_ids,
      });
    }
    await loadFFS();
  }

  async function handleDelete(entry: FFSEntry) {
    if (!confirm(`Delete "${entry.label || entry.lp_url || entry.entry_id}"?\n\nLinked lines will lose this FFS (override lines keep their custom values).`)) {
      return;
    }
    setError(null);
    try {
      await api.ffs.delete(code, entry.entry_id);
      await loadFFS();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete FFS entry");
    }
  }

  return (
    <>
      <Card>
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-bold text-fg">Form Friction Scores</h3>
            <p className="mt-1 text-xs text-fg-muted">
              Score each form once. The diagnostic engine uses FFS to adjust CVR benchmarks
              for the F2, F4, and C1 signals.
            </p>
          </div>
          <button
            onClick={() => { setEditingEntry(null); setWizardOpen(true); }}
            className="flex items-center gap-1.5 rounded-sm border-2 border-line px-3 py-1.5 text-xs font-bold text-fg transition-colors hover:border-line-strong"
          >
            <Plus className="h-3.5 w-3.5" /> New FFS entry
          </button>
        </div>

        {error && (
          <div className="mt-3 rounded-sm border-2 border-tint-danger bg-tint-danger px-3 py-2 text-xs text-danger">
            {error}
          </div>
        )}

        {loading ? (
          <div className="mt-4 h-16 animate-pulse rounded-sm bg-surface-sunken" />
        ) : entries.length === 0 ? (
          <div className="mt-4 rounded-sm border-2 border-dashed border-line px-4 py-6 text-center">
            <FileText className="mx-auto h-6 w-6 text-fg-faint" />
            <p className="mt-2 text-sm text-fg-muted">No FFS entries yet</p>
            <p className="mt-1 text-xs text-fg-faint">
              Add one for each unique form on this campaign.
            </p>
          </div>
        ) : (
          <div className="mt-4 space-y-2">
            {entries.map((entry) => {
              const bucket = ffsBucket(entry.ffs_score);
              return (
                <div
                  key={entry.entry_id}
                  className="flex items-center gap-3 rounded-sm border-2 border-line-soft bg-surface-sunken px-3 py-2.5"
                >
                  <div className={cn("flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-sm border", bucket.bg, bucket.ring)}>
                    <span className={cn("tnum font-mono text-sm font-bold", bucket.color)}>
                      {entry.ffs_score.toFixed(0)}
                    </span>
                  </div>
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium text-fg">
                      {entry.label || entry.lp_url || (entry.is_platform_form ? `${entry.platform_id ?? "platform"} form` : "Untitled form")}
                    </p>
                    <p className="text-xs text-fg-muted">
                      {entry.is_platform_form
                        ? `In-platform (${entry.platform_id ?? "—"})`
                        : entry.lp_url ?? "Landing page form"}
                      {" · "}
                      {entry.linked_line_count} line{entry.linked_line_count === 1 ? "" : "s"}
                      {" · "}
                      <span className={bucket.color}>{bucket.label}</span>
                    </p>
                  </div>
                  <button
                    onClick={() => { setEditingEntry(entry); setWizardOpen(true); }}
                    className="flex-shrink-0 rounded-sm p-1 text-fg-muted transition-colors hover:bg-surface-card hover:text-fg"
                    title="Edit"
                  >
                    <Pencil className="h-3.5 w-3.5" />
                  </button>
                  <button
                    onClick={() => handleDelete(entry)}
                    className="flex-shrink-0 rounded-sm p-1 text-fg-faint transition-colors hover:bg-tint-danger hover:text-danger"
                    title="Delete"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </div>
              );
            })}
          </div>
        )}
      </Card>

      <FFSWizard
        open={wizardOpen}
        mode={editingEntry ? "edit" : "create"}
        projectCode={code}
        entry={editingEntry}
        lines={lines}
        existingLineLinks={lineLinks}
        onClose={() => { setWizardOpen(false); setEditingEntry(null); }}
        onSubmit={handleSubmit}
      />
    </>
  );
}
