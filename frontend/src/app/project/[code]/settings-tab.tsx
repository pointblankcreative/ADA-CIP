"use client";

import { useEffect, useState, useCallback } from "react";
import { Plus, Trash2, Globe, Loader2 } from "lucide-react";
import { api, type GA4Url, type GA4Property } from "@/lib/api";
import { Card } from "@/components/card";

export function SettingsTab({ code }: { code: string }) {
  const [urls, setUrls] = useState<GA4Url[]>([]);
  const [properties, setProperties] = useState<GA4Property[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ ga4_property_id: "", url_pattern: "", label: "" });

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [u, p] = await Promise.all([
        api.ga4.urls(code),
        api.ga4.properties(),
      ]);
      setUrls(u);
      setProperties(p);
    } catch {
      /* ignore */
    } finally {
      setLoading(false);
    }
  }, [code]);

  useEffect(() => { loadData(); }, [loadData]);

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault();
    if (!form.ga4_property_id || !form.url_pattern) return;
    setAdding(true);
    try {
      const url = await api.ga4.addUrl(code, {
        ga4_property_id: form.ga4_property_id,
        url_pattern: form.url_pattern,
        label: form.label || undefined,
      });
      setUrls((prev) => [...prev, url]);
      setForm({ ga4_property_id: form.ga4_property_id, url_pattern: "", label: "" });
      setShowForm(false);
    } catch {
      /* ignore */
    } finally {
      setAdding(false);
    }
  }

  async function handleDelete(urlId: string) {
    try {
      await api.ga4.deleteUrl(code, urlId);
      setUrls((prev) => prev.filter((u) => u.id !== urlId));
    } catch {
      /* ignore */
    }
  }

  if (loading) {
    return (
      <Card className="animate-pulse">
        <div className="h-4 w-32 rounded bg-slate-700" />
        <div className="mt-4 h-24 rounded bg-slate-700/50" />
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <Card>
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium text-white">GA4 Web Analytics URLs</h3>
            <p className="mt-1 text-xs text-slate-500">
              Configure landing page URLs to pull GA4 web analytics data into the performance view.
            </p>
          </div>
          {!showForm && (
            <button
              onClick={() => setShowForm(true)}
              className="flex items-center gap-1.5 rounded-md border border-slate-700 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-300 hover:bg-slate-700 transition-colors"
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
                className="flex items-center gap-3 rounded-md border border-slate-800 bg-slate-900/50 px-3 py-2.5"
              >
                <Globe className="h-4 w-4 flex-shrink-0 text-slate-600" />
                <div className="min-w-0 flex-1">
                  <p className="truncate text-sm text-slate-200">{u.url_pattern}</p>
                  <p className="text-xs text-slate-500">
                    GA4: {u.ga4_property_id}
                    {u.label && ` · ${u.label}`}
                  </p>
                </div>
                <button
                  onClick={() => handleDelete(u.id)}
                  className="flex-shrink-0 rounded p-1 text-slate-600 hover:bg-red-500/10 hover:text-red-400 transition-colors"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              </div>
            ))}
          </div>
        )}

        {urls.length === 0 && !showForm && (
          <div className="mt-4 rounded-md border border-dashed border-slate-700 px-4 py-6 text-center">
            <Globe className="mx-auto h-6 w-6 text-slate-600" />
            <p className="mt-2 text-sm text-slate-500">No GA4 URLs configured</p>
            <p className="mt-1 text-xs text-slate-600">
              Add landing page URLs to see web analytics data on the performance tab.
            </p>
          </div>
        )}

        {/* Add URL form */}
        {showForm && (
          <form onSubmit={handleAdd} className="mt-4 space-y-3 rounded-md border border-slate-700 bg-slate-900/50 p-4">
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                GA4 Property
              </label>
              {properties.length > 0 ? (
                <select
                  value={form.ga4_property_id}
                  onChange={(e) => setForm((f) => ({ ...f, ga4_property_id: e.target.value }))}
                  className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-brand-600"
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
                  className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
                />
              )}
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                URL Pattern
              </label>
              <input
                required
                placeholder="e.g. example.com/campaign-landing-page"
                value={form.url_pattern}
                onChange={(e) => setForm((f) => ({ ...f, url_pattern: e.target.value }))}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
              <p className="mt-1 text-xs text-slate-600">
                Partial URL match — e.g. &quot;bcgeu.ca/bargaining&quot; matches all pages containing this path.
              </p>
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                Label <span className="text-slate-600">(optional)</span>
              </label>
              <input
                placeholder="e.g. Main landing page"
                value={form.label}
                onChange={(e) => setForm((f) => ({ ...f, label: e.target.value }))}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
            </div>
            <div className="flex gap-2 pt-1">
              <button
                type="submit"
                disabled={adding || !form.ga4_property_id || !form.url_pattern}
                className="flex items-center gap-1.5 rounded-md bg-brand-600 px-4 py-2 text-sm font-medium text-white hover:bg-brand-500 disabled:opacity-50 transition-colors"
              >
                {adding && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                Add URL
              </button>
              <button
                type="button"
                onClick={() => setShowForm(false)}
                className="rounded-md border border-slate-700 px-4 py-2 text-sm text-slate-400 hover:bg-slate-800 transition-colors"
              >
                Cancel
              </button>
            </div>
          </form>
        )}
      </Card>
    </div>
  );
}
