"use client";

import { useState, useEffect, Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { ArrowLeft, Loader2, CheckCircle2 } from "lucide-react";
import Link from "next/link";
import { api, type ProjectCreatePayload } from "@/lib/api";
import { Card } from "@/components/card";

export default function NewProjectPage() {
  return (
    <Suspense fallback={<div className="text-slate-400 text-sm">Loading...</div>}>
      <NewProjectForm />
    </Suspense>
  );
}

function NewProjectForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const prefillCode = searchParams.get("code") ?? "";

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<{
    code: string;
    mediaPlanSynced: boolean;
  } | null>(null);
  const [form, setForm] = useState<ProjectCreatePayload>({
    project_code: prefillCode,
    client_name: "",
    project_name: "",
    start_date: "",
    end_date: "",
    net_budget: 0,
    media_plan_sheet_url: "",
    slack_channel_id: "",
  });

  useEffect(() => {
    if (prefillCode) {
      setForm((prev) => ({ ...prev, project_code: prefillCode }));
    }
  }, [prefillCode]);

  const set = (field: keyof ProjectCreatePayload, value: string | number) =>
    setForm((prev) => ({ ...prev, [field]: value }));

  const codeValid = /^2[0-9]\d{3}$/.test(form.project_code);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    setSuccess(null);
    try {
      const payload: ProjectCreatePayload = {
        ...form,
        media_plan_sheet_url: form.media_plan_sheet_url || undefined,
        slack_channel_id: form.slack_channel_id || undefined,
      };
      const result = await api.admin.projects.create(payload);
      const mediaSynced = result?.media_plan_sync?.status === "success";
      setSuccess({ code: form.project_code, mediaPlanSynced: mediaSynced });
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create project");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="max-w-2xl">
      <Link
        href="/admin"
        className="inline-flex items-center gap-1.5 text-sm text-slate-400 hover:text-white transition-colors mb-4"
      >
        <ArrowLeft className="h-4 w-4" /> Admin
      </Link>

      <h1 className="text-xl font-semibold text-white">Create New Project</h1>
      <p className="mt-1 text-sm text-slate-400">
        The project will be created, optionally synced with a media plan, and made visible in the dashboard.
      </p>

      <form onSubmit={handleSubmit} className="mt-6 space-y-5">
        <Card>
          <div className="space-y-4">
            {/* Project Code */}
            <div>
              <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                Project Code <span className="text-red-400">*</span>
              </label>
              <input
                required
                placeholder="e.g. 26015"
                value={form.project_code}
                onChange={(e) => set("project_code", e.target.value)}
                className={`w-full rounded-md border bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600 ${
                  form.project_code && !codeValid
                    ? "border-red-500"
                    : "border-slate-700"
                }`}
              />
              {form.project_code && !codeValid && (
                <p className="mt-1 text-xs text-red-400">Must be YYNNN format (e.g. 25013, 26009)</p>
              )}
            </div>

            {/* Client Name */}
            <div>
              <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                Client Name <span className="text-red-400">*</span>
              </label>
              <input
                required
                placeholder="e.g. BCGEU"
                value={form.client_name}
                onChange={(e) => set("client_name", e.target.value)}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
            </div>

            {/* Project Name */}
            <div>
              <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                Project Name <span className="text-red-400">*</span>
              </label>
              <input
                required
                placeholder="e.g. BCGEU Bargaining Escalation"
                value={form.project_name}
                onChange={(e) => set("project_name", e.target.value)}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
            </div>

            {/* Dates */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                  Start Date <span className="text-red-400">*</span>
                </label>
                <input
                  required
                  type="date"
                  value={form.start_date}
                  onChange={(e) => set("start_date", e.target.value)}
                  className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-brand-600"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                  End Date <span className="text-red-400">*</span>
                </label>
                <input
                  required
                  type="date"
                  value={form.end_date}
                  onChange={(e) => set("end_date", e.target.value)}
                  className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-brand-600"
                />
              </div>
            </div>

            {/* Budget */}
            <div>
              <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                Net Budget (CAD) <span className="text-red-400">*</span>
              </label>
              <input
                required
                type="number"
                min={0}
                step={0.01}
                placeholder="e.g. 50000"
                value={form.net_budget || ""}
                onChange={(e) => set("net_budget", parseFloat(e.target.value) || 0)}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
            </div>
          </div>
        </Card>

        <Card>
          <h2 className="text-sm font-medium text-white mb-4">Optional Configuration</h2>
          <div className="space-y-4">
            {/* Media Plan Sheet */}
            <div>
              <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                Media Plan Sheet URL
              </label>
              <input
                placeholder="https://docs.google.com/spreadsheets/d/..."
                value={form.media_plan_sheet_url}
                onChange={(e) => set("media_plan_sheet_url", e.target.value)}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
              <p className="mt-1 text-xs text-slate-500">
                If provided, the media plan will be synced automatically.
              </p>
            </div>

            {/* Slack Channel */}
            <div>
              <label className="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                Slack Channel
              </label>
              <input
                placeholder="e.g. #cip-bcgeu or C06ABC123"
                value={form.slack_channel_id}
                onChange={(e) => set("slack_channel_id", e.target.value)}
                className="w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-brand-600"
              />
              <p className="mt-1 text-xs text-slate-500">
                Channel name or ID for pacing alerts. Defaults to #cip-alerts if empty.
              </p>
            </div>
          </div>
        </Card>

        {error && (
          <div className="rounded-md border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-400">
            {error}
          </div>
        )}

        {success && (
          <div className="rounded-md border border-emerald-500/30 bg-emerald-500/10 px-5 py-4">
            <div className="flex items-center gap-2 text-sm font-medium text-emerald-400">
              <CheckCircle2 className="h-4 w-4" />
              Project {success.code} created successfully
            </div>
            <p className="mt-1 text-xs text-emerald-400/70">
              {success.mediaPlanSynced
                ? "Media plan synced. Pacing and alerts are now active."
                : "No media plan linked — you can add one later from the project management page."}
            </p>
            <div className="mt-3 flex gap-3">
              <Link
                href={`/project/${success.code}`}
                className="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 transition-colors"
              >
                View Project
              </Link>
              <Link
                href="/admin/projects"
                className="rounded-md border border-slate-700 px-4 py-2 text-sm text-slate-300 hover:bg-slate-800 transition-colors"
              >
                Manage Projects
              </Link>
            </div>
          </div>
        )}

        {!success && (
          <button
            type="submit"
            disabled={submitting || !codeValid || !form.client_name || !form.project_name}
            className="flex items-center gap-2 rounded-md bg-brand-600 px-5 py-2.5 text-sm font-medium text-white hover:bg-brand-500 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {submitting && <Loader2 className="h-4 w-4 animate-spin" />}
            {submitting ? "Creating..." : "Create Project"}
          </button>
        )}
      </form>
    </div>
  );
}
