"use client";

import { useState, useEffect, Suspense, useCallback } from "react";
import { useSearchParams } from "next/navigation";
import { ArrowLeft, Loader2, CheckCircle2, Copy, Check } from "lucide-react";
import Link from "next/link";
import { api, type ProjectCreatePayload } from "@/lib/api";
import { Card } from "@/components/card";
import { cn } from "@/lib/utils";

const SERVICE_ACCOUNT_EMAIL = "cip-sheets-reader@point-blank-ada.iam.gserviceaccount.com";

const INPUT_CLS =
  "w-full rounded-sm border-2 border-line bg-surface-sunken px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none focus:border-accent";

export default function NewProjectPage() {
  return (
    <Suspense fallback={<div className="text-sm text-fg-muted">Loading...</div>}>
      <NewProjectForm />
    </Suspense>
  );
}

function NewProjectForm() {
  const searchParams = useSearchParams();
  const prefillCode = searchParams.get("code") ?? "";

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<{
    code: string;
    mediaPlanStatus: "success" | "error" | "skipped";
    mediaPlanMessage?: string;
    linesCreated?: number;
  } | null>(null);
  const [form, setForm] = useState<ProjectCreatePayload>({
    project_code: prefillCode,
    client_name: "",
    project_name: "",
    start_date: "",
    end_date: "",
    net_budget: 0,
    media_plan_sheet_url: "",
    media_plan_tab_name: "",
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
        media_plan_tab_name: form.media_plan_tab_name || undefined,
        slack_channel_id: form.slack_channel_id || undefined,
      };
      const result = await api.admin.projects.create(payload);
      const syncStatus = result?.media_plan_sync?.status ?? "skipped";
      setSuccess({
        code: form.project_code,
        mediaPlanStatus: syncStatus,
        mediaPlanMessage: result?.media_plan_sync?.message,
        linesCreated: result?.media_plan_sync?.lines_created,
      });
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
        className="mb-4 inline-flex items-center gap-1.5 font-mono text-[11px] uppercase tracking-[0.08em] text-fg-muted transition-colors hover:text-fg"
      >
        <ArrowLeft className="h-3.5 w-3.5" /> Admin
      </Link>

      <h1 className="text-xl font-extrabold tracking-tight text-fg">Create New Project</h1>
      <p className="mt-1 text-sm text-fg-muted">
        The project will be created, optionally synced with a media plan, and made visible in the dashboard.
      </p>

      <form onSubmit={handleSubmit} className="mt-6 space-y-5">
        <Card>
          <div className="space-y-4">
            {/* Project Code */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Project Code <span className="text-danger">*</span>
              </label>
              <input
                required
                placeholder="e.g. 26015"
                value={form.project_code}
                onChange={(e) => set("project_code", e.target.value)}
                className={cn(
                  INPUT_CLS,
                  "font-mono",
                  form.project_code && !codeValid && "border-danger"
                )}
              />
              {form.project_code && !codeValid && (
                <p className="mt-1 text-xs text-danger">Must be YYNNN format (e.g. 25013, 26009)</p>
              )}
            </div>

            {/* Client Name */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Client Name <span className="text-danger">*</span>
              </label>
              <input
                required
                placeholder="e.g. BCGEU"
                value={form.client_name}
                onChange={(e) => set("client_name", e.target.value)}
                className={INPUT_CLS}
              />
            </div>

            {/* Project Name */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Project Name <span className="text-danger">*</span>
              </label>
              <input
                required
                placeholder="e.g. BCGEU Bargaining Escalation"
                value={form.project_name}
                onChange={(e) => set("project_name", e.target.value)}
                className={INPUT_CLS}
              />
            </div>

            {/* Dates */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="label mb-1.5 block text-[10px]">
                  Start Date <span className="text-danger">*</span>
                </label>
                <input
                  required
                  type="date"
                  value={form.start_date}
                  onChange={(e) => set("start_date", e.target.value)}
                  className={INPUT_CLS}
                />
              </div>
              <div>
                <label className="label mb-1.5 block text-[10px]">
                  End Date <span className="text-danger">*</span>
                </label>
                <input
                  required
                  type="date"
                  value={form.end_date}
                  onChange={(e) => set("end_date", e.target.value)}
                  className={INPUT_CLS}
                />
              </div>
            </div>

            {/* Budget */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Net Budget (CAD) <span className="text-danger">*</span>
              </label>
              <input
                required
                type="number"
                min={0}
                step={0.01}
                placeholder="e.g. 50000"
                value={form.net_budget || ""}
                onChange={(e) => set("net_budget", parseFloat(e.target.value) || 0)}
                className={cn(INPUT_CLS, "tnum font-mono")}
              />
            </div>
          </div>
        </Card>

        <Card>
          <h2 className="mb-4 text-sm font-bold text-fg">Optional Configuration</h2>
          <div className="space-y-4">
            {/* Media Plan Sheet */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Media Plan Sheet URL
              </label>
              <input
                placeholder="https://docs.google.com/spreadsheets/d/..."
                value={form.media_plan_sheet_url}
                onChange={(e) => set("media_plan_sheet_url", e.target.value)}
                className={cn(INPUT_CLS, "font-mono text-[13px]")}
              />
              <p className="mt-1 text-xs text-fg-muted">
                If provided, the media plan will be synced automatically.
              </p>
              <MediaPlanSharingInstructions />
            </div>

            {/* Media Plan Tab Name */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Media Plan Tab Name
              </label>
              <input
                placeholder="e.g. Media Plan V2"
                value={form.media_plan_tab_name}
                onChange={(e) => set("media_plan_tab_name", e.target.value)}
                className={INPUT_CLS}
              />
              <p className="mt-1 text-xs text-fg-muted">
                If set, only this tab will be synced. Leave blank to sync all matching tabs.
              </p>
            </div>

            {/* Slack Channel */}
            <div>
              <label className="label mb-1.5 block text-[10px]">
                Slack Channel
              </label>
              <input
                placeholder="e.g. #cip-bcgeu or C06ABC123"
                value={form.slack_channel_id}
                onChange={(e) => set("slack_channel_id", e.target.value)}
                className={cn(INPUT_CLS, "font-mono text-[13px]")}
              />
              <p className="mt-1 text-xs text-fg-muted">
                Channel name or ID for pacing alerts. Defaults to #cip-alerts if empty.
              </p>
            </div>
          </div>
        </Card>

        {error && (
          <div className="rounded-md border-2 border-tint-danger bg-tint-danger px-4 py-3 text-sm text-danger">
            {error}
          </div>
        )}

        {success && (
          <div
            className={cn(
              "rounded-md border-2 px-5 py-4",
              success.mediaPlanStatus === "error"
                ? "border-tint-warn bg-tint-warn"
                : "border-tint-ok bg-tint-ok"
            )}
          >
            <div
              className={cn(
                "flex items-center gap-2 text-sm font-bold",
                success.mediaPlanStatus === "error" ? "text-warn" : "text-ok"
              )}
            >
              <CheckCircle2 className="h-4 w-4" />
              Project {success.code} created successfully
            </div>
            <p className="mt-1 text-xs text-fg-secondary">
              {success.mediaPlanStatus === "success"
                ? `Media plan synced successfully — ${success.linesCreated ?? 0} line items imported. Pacing and alerts are now active.`
                : success.mediaPlanStatus === "error"
                  ? `Project created, but media plan sync failed: ${success.mediaPlanMessage || "unknown error"}. Make sure the spreadsheet is shared with ${SERVICE_ACCOUNT_EMAIL}`
                  : "No media plan linked — you can add one later from the project management page."}
            </p>
            <div className="mt-3 flex gap-3">
              <Link
                href={`/project/${success.code}`}
                className="rounded-sm border-2 border-accent bg-accent px-4 py-2 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover"
              >
                View Project
              </Link>
              <Link
                href="/admin/projects"
                className="rounded-sm border-2 border-line px-4 py-2 text-sm font-bold text-fg transition-colors hover:border-line-strong"
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
            className="flex items-center gap-2 rounded-sm border-2 border-accent bg-accent px-5 py-2.5 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover disabled:cursor-not-allowed disabled:opacity-50"
          >
            {submitting && <Loader2 className="h-4 w-4 animate-spin" />}
            {submitting ? "Creating..." : "Create Project"}
          </button>
        )}
      </form>
    </div>
  );
}

function MediaPlanSharingInstructions() {
  const [copied, setCopied] = useState(false);

  const copyEmail = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(SERVICE_ACCOUNT_EMAIL);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* fallback: do nothing */
    }
  }, []);

  return (
    <div className="mt-3 rounded-md border-2 border-tint-info bg-tint-info px-4 py-3">
      <p className="text-xs font-bold text-info">
        Share your media plan with ADA
      </p>
      <p className="mt-1 text-xs text-fg-secondary">
        To allow ADA to read this spreadsheet, share it as <strong className="text-fg">Viewer</strong> with:
      </p>
      <div className="mt-2 flex items-center gap-2">
        <code className="flex-1 select-all rounded-sm border border-line-soft bg-surface-card px-2.5 py-1.5 font-mono text-xs text-info">
          {SERVICE_ACCOUNT_EMAIL}
        </code>
        <button
          type="button"
          onClick={copyEmail}
          className="flex-shrink-0 rounded-sm border-2 border-line px-2.5 py-1.5 text-xs font-bold text-fg transition-colors hover:border-line-strong"
        >
          {copied ? (
            <span className="flex items-center gap-1 text-ok">
              <Check className="h-3 w-3" /> Copied
            </span>
          ) : (
            <span className="flex items-center gap-1">
              <Copy className="h-3 w-3" /> Copy
            </span>
          )}
        </button>
      </div>
    </div>
  );
}
