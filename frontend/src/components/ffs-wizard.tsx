"use client";

/**
 * FFS Wizard modal — create or edit a Form Friction Score entry.
 *
 * Arch A (landing page form): full wizard — LP URL + all 7 friction questions.
 * Arch B (in-platform lead form): skip LP questions, show platform picker + a
 *   subset of inputs. The `-5` platform-form discount is auto-applied.
 *
 * Live preview recomputes client-side via `computeFFS` on every keystroke;
 * the server recomputes authoritatively on submit.
 */

import { useEffect, useMemo, useState } from "react";
import { X, Loader2, Info, Plus } from "lucide-react";
import type {
  FFSEntry,
  FFSInputs,
  PacingLine,
} from "@/lib/api";
import {
  computeFFS,
  ffsBucket,
  FIELD_TYPE_FRICTION,
  FIELD_TYPE_LABELS,
  DEFAULT_FFS_INPUTS,
} from "@/lib/ffs";
import { cn, platformLabel } from "@/lib/utils";

type Mode = "create" | "edit";

interface Props {
  open: boolean;
  mode: Mode;
  projectCode: string;
  /** Existing entry when mode="edit". */
  entry?: FFSEntry | null;
  /** All lines on this project — used for the apply-to-lines picker. */
  lines: PacingLine[];
  /** Entry IDs already linked to each line, keyed by line_id. */
  existingLineLinks?: Record<string, string | null>;
  onClose: () => void;
  /** Caller handles the create/update + apply network calls. */
  onSubmit: (payload: {
    label: string | null;
    lp_url: string | null;
    is_platform_form: boolean;
    platform_id: string | null;
    ffs_inputs: FFSInputs;
    applied_line_ids: string[];
  }) => Promise<void>;
}

const PLATFORM_OPTIONS = [
  { id: "meta", label: "Meta (Facebook/Instagram)" },
  { id: "linkedin", label: "LinkedIn" },
  { id: "tiktok", label: "TikTok" },
  { id: "google", label: "Google Ads" },
];

const INPUT_CLS =
  "w-full rounded-sm border-2 border-line bg-surface-sunken px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none focus:border-accent";

export function FFSWizard({
  open,
  mode,
  entry,
  lines,
  existingLineLinks = {},
  onClose,
  onSubmit,
}: Props) {
  const [label, setLabel] = useState("");
  const [lpUrl, setLpUrl] = useState("");
  const [isPlatformForm, setIsPlatformForm] = useState(false);
  const [platformId, setPlatformId] = useState<string | null>(null);
  const [inputs, setInputs] = useState<FFSInputs>(DEFAULT_FFS_INPUTS);
  const [appliedLineIds, setAppliedLineIds] = useState<string[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Seed state from `entry` when editing (or reset on open for create).
  useEffect(() => {
    if (!open) return;
    if (entry) {
      setLabel(entry.label ?? "");
      setLpUrl(entry.lp_url ?? "");
      setIsPlatformForm(entry.is_platform_form);
      setPlatformId(entry.platform_id);
      setInputs({ ...DEFAULT_FFS_INPUTS, ...entry.ffs_inputs });
      setAppliedLineIds(
        Object.entries(existingLineLinks)
          .filter(([, entryId]) => entryId === entry.entry_id)
          .map(([lineId]) => lineId)
      );
    } else {
      setLabel("");
      setLpUrl("");
      setIsPlatformForm(false);
      setPlatformId(null);
      setInputs(DEFAULT_FFS_INPUTS);
      setAppliedLineIds([]);
    }
    setError(null);
  }, [open, entry, existingLineLinks]);

  const previewScore = useMemo(() => {
    // When the top-level flag is set, auto-apply the -5 discount in preview
    // (the backend does the same on submit).
    return computeFFS({ ...inputs, is_platform_form: isPlatformForm || inputs.is_platform_form });
  }, [inputs, isPlatformForm]);

  const bucket = ffsBucket(previewScore);

  if (!open) return null;

  function addFieldType(ft: string) {
    // Each click adds one instance. Remove via chip X below.
    setInputs((prev) => ({ ...prev, field_types: [...prev.field_types, ft] }));
  }

  function removeFieldTypeAt(idx: number) {
    setInputs((prev) => ({
      ...prev,
      field_types: prev.field_types.filter((_, i) => i !== idx),
    }));
  }

  function toggleLine(lineId: string) {
    setAppliedLineIds((prev) =>
      prev.includes(lineId) ? prev.filter((id) => id !== lineId) : [...prev, lineId]
    );
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await onSubmit({
        label: label.trim() || null,
        lp_url: isPlatformForm ? null : (lpUrl.trim() || null),
        is_platform_form: isPlatformForm,
        platform_id: isPlatformForm ? platformId : null,
        ffs_inputs: inputs,
        applied_line_ids: appliedLineIds,
      });
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save FFS entry");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: "color-mix(in srgb, var(--dark-900) 62%, transparent)" }}
      onClick={onClose}
    >
      <div
        className="flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-lg border-2 border-line bg-surface-card shadow-soft"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b-2 border-line-soft px-6 py-4">
          <div>
            <h2 className="text-base font-bold text-fg">
              {mode === "edit" ? "Edit Form Friction entry" : "New Form Friction entry"}
            </h2>
            <p className="mt-0.5 text-xs text-fg-muted">
              Score this form once; apply it to every line using it.
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-sm p-1.5 text-fg-muted transition-colors hover:bg-surface-sunken hover:text-fg"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto">
          <div className="grid grid-cols-1 gap-6 p-6 md:grid-cols-[1fr_260px]">
            {/* ── Left: inputs ─────────────────────────────────────── */}
            <div className="min-w-0 space-y-5">
              {/* Arch A/B toggle */}
              <div>
                <label className="label mb-1.5 block text-[10px]">
                  Form type
                </label>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => setIsPlatformForm(false)}
                    className={cn(
                      "rounded-sm border-2 px-3 py-2 text-left text-sm transition-colors duration-fast",
                      !isPlatformForm
                        ? "border-accent bg-tint-accent text-fg"
                        : "border-line-soft bg-surface-sunken text-fg-muted hover:border-line"
                    )}
                  >
                    <div className="font-bold">Landing page form</div>
                    <div className="mt-0.5 text-xs opacity-70">Hosted on your LP</div>
                  </button>
                  <button
                    type="button"
                    onClick={() => setIsPlatformForm(true)}
                    className={cn(
                      "rounded-sm border-2 px-3 py-2 text-left text-sm transition-colors duration-fast",
                      isPlatformForm
                        ? "border-accent bg-tint-accent text-fg"
                        : "border-line-soft bg-surface-sunken text-fg-muted hover:border-line"
                    )}
                  >
                    <div className="font-bold">In-platform lead form</div>
                    <div className="mt-0.5 text-xs opacity-70">Meta, LinkedIn, TikTok</div>
                  </button>
                </div>
              </div>

              {/* Label + URL (or platform picker for Arch B) */}
              <div className="grid grid-cols-1 gap-3">
                <div>
                  <label className="label mb-1 block text-[10px]">
                    Label <span className="normal-case text-fg-faint">(optional)</span>
                  </label>
                  <input
                    placeholder="e.g. underfunded.ca signup form"
                    value={label}
                    onChange={(e) => setLabel(e.target.value)}
                    className={INPUT_CLS}
                  />
                </div>

                {!isPlatformForm ? (
                  <div>
                    <label className="label mb-1 block text-[10px]">
                      Landing page URL
                    </label>
                    <input
                      placeholder="https://underfunded.ca/signup"
                      value={lpUrl}
                      onChange={(e) => setLpUrl(e.target.value)}
                      className={cn(INPUT_CLS, "font-mono text-[13px]")}
                    />
                  </div>
                ) : (
                  <div>
                    <label className="label mb-1 block text-[10px]">
                      Platform
                    </label>
                    <select
                      value={platformId ?? ""}
                      onChange={(e) => setPlatformId(e.target.value || null)}
                      className={INPUT_CLS}
                    >
                      <option value="">Select platform…</option>
                      {PLATFORM_OPTIONS.map((p) => (
                        <option key={p.id} value={p.id}>{p.label}</option>
                      ))}
                    </select>
                  </div>
                )}
              </div>

              {/* Friction inputs */}
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-3">
                  <NumberField
                    label="Total fields"
                    value={inputs.field_count}
                    min={0}
                    max={40}
                    onChange={(v) =>
                      setInputs((p) => ({
                        ...p,
                        field_count: v,
                        // Clamp required_fields if it exceeds new field_count
                        required_fields: Math.min(p.required_fields, v),
                      }))
                    }
                  />
                  <NumberField
                    label="Required fields"
                    value={inputs.required_fields}
                    min={0}
                    max={inputs.field_count}
                    onChange={(v) => setInputs((p) => ({ ...p, required_fields: v }))}
                  />
                </div>

                {/* Field type picker */}
                <div>
                  <label className="label mb-1.5 block text-[10px]">
                    Field types present
                  </label>
                  <div className="grid grid-cols-2 gap-1.5">
                    {Object.keys(FIELD_TYPE_FRICTION).map((ft) => (
                      <button
                        key={ft}
                        type="button"
                        onClick={() => addFieldType(ft)}
                        className="flex items-center justify-between rounded-sm border border-line-soft bg-surface-sunken px-2.5 py-1.5 text-xs text-fg-secondary transition-colors duration-fast hover:border-accent hover:bg-tint-accent"
                      >
                        <span className="truncate">{FIELD_TYPE_LABELS[ft]}</span>
                        <Plus className="h-3 w-3 flex-shrink-0 text-fg-faint" />
                      </button>
                    ))}
                  </div>
                  {inputs.field_types.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {inputs.field_types.map((ft, idx) => (
                        <button
                          key={idx}
                          type="button"
                          onClick={() => removeFieldTypeAt(idx)}
                          className="flex items-center gap-1 rounded-pill border border-line-soft bg-surface-sunken px-2 py-0.5 text-xs text-fg-secondary transition-colors hover:bg-tint-danger hover:text-danger"
                        >
                          {FIELD_TYPE_LABELS[ft] ?? ft}
                          <X className="h-2.5 w-2.5" />
                        </button>
                      ))}
                    </div>
                  )}
                  <p className="mt-1 text-xs text-fg-faint">
                    Click a type to add it; click a chip to remove. Count per instance (e.g. two phone fields → click &quot;Phone&quot; twice).
                  </p>
                </div>

                <NumberField
                  label="Clicks from LP load to form submit"
                  value={inputs.clicks_to_submit}
                  min={1}
                  max={10}
                  onChange={(v) => setInputs((p) => ({ ...p, clicks_to_submit: v }))}
                />

                <div className="space-y-2">
                  <Checkbox
                    label="Form is below the fold on mobile"
                    checked={inputs.below_fold_mobile}
                    onChange={(v) => setInputs((p) => ({ ...p, below_fold_mobile: v }))}
                  />
                  <Checkbox
                    label="Browser autofill works (standard field names)"
                    checked={inputs.has_autofill}
                    onChange={(v) => setInputs((p) => ({ ...p, has_autofill: v }))}
                  />
                </div>
              </div>

              {/* Apply-to-lines picker */}
              {lines.length > 0 && (
                <div>
                  <label className="label mb-1.5 block text-[10px]">
                    Apply to media plan lines
                    <span className="ml-2 normal-case text-fg-faint">
                      ({appliedLineIds.length} selected)
                    </span>
                  </label>
                  <div className="max-h-48 overflow-y-auto rounded-sm border-2 border-line-soft bg-surface-sunken">
                    {lines.map((line) => {
                      const linkedTo = existingLineLinks[line.line_id];
                      const linkedToOther = linkedTo && entry?.entry_id !== linkedTo;
                      const selected = appliedLineIds.includes(line.line_id);
                      return (
                        <label
                          key={line.line_id}
                          className={cn(
                            "flex cursor-pointer items-center gap-2.5 border-b border-line-soft px-3 py-2 last:border-b-0 hover:bg-surface-card",
                            selected && "bg-tint-accent"
                          )}
                        >
                          <input
                            type="checkbox"
                            checked={selected}
                            onChange={() => toggleLine(line.line_id)}
                            className="h-3.5 w-3.5 rounded-xs border-line accent-[var(--accent)]"
                          />
                          <div className="min-w-0 flex-1">
                            <p className="truncate text-xs font-medium text-fg">
                              {line.audience_name || line.line_code || line.line_id}
                            </p>
                            <p className="truncate font-mono text-[10px] text-fg-muted">
                              {platformLabel(line.platform_id)} · {line.channel_category}
                            </p>
                          </div>
                          {linkedToOther && (
                            <span className="flex-shrink-0 rounded-xs bg-tint-warn px-1.5 py-0.5 font-mono text-[10px] text-warn">
                              already linked
                            </span>
                          )}
                        </label>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>

            {/* ── Right: live preview ──────────────────────────────── */}
            <aside className="space-y-3">
              <div
                className={cn(
                  "rounded-md border-2 px-4 py-4",
                  bucket.bg,
                  bucket.ring
                )}
              >
                <p className="label text-[10px]">
                  Form Friction Score
                </p>
                <p className={cn("tnum mt-1 font-display text-4xl", bucket.color)}>
                  {previewScore.toFixed(0)}
                </p>
                <p className={cn("font-mono text-xs font-semibold uppercase tracking-[0.06em]", bucket.color)}>
                  {bucket.label}
                </p>
                <div className="mt-3 h-1.5 overflow-hidden rounded-pill bg-surface-sunken">
                  <div
                    className={cn("h-full transition-all duration-base ease-snap", bucket.fill)}
                    style={{ width: `${previewScore}%` }}
                  />
                </div>
                <div className="mt-1 flex justify-between font-mono text-[9px] text-fg-faint">
                  <span>0</span><span>50</span><span>100</span>
                </div>
              </div>

              <div className="rounded-sm border-2 border-line-soft bg-surface-sunken p-3 text-xs text-fg-secondary">
                <div className="mb-1.5 flex items-center gap-1.5 text-fg-muted">
                  <Info className="h-3 w-3" />
                  <span className="font-bold">How this is used</span>
                </div>
                <p className="leading-relaxed">
                  FFS adjusts the CVR benchmark used by the F2/F4/C1 diagnostic signals so a
                  high-friction form isn&apos;t judged against the same bar as a 2-field Meta form.
                </p>
              </div>

              {isPlatformForm && (
                <div className="rounded-sm border-2 border-tint-accent bg-tint-accent p-3 text-xs font-medium text-accent-ink">
                  Platform-form discount (−5) auto-applied.
                </div>
              )}
            </aside>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between gap-3 border-t-2 border-line-soft bg-surface-sunken px-6 py-3">
            <div className="flex-1 text-xs text-danger">{error}</div>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={onClose}
                disabled={submitting}
                className="rounded-sm border-2 border-line px-4 py-2 text-sm font-bold text-fg transition-colors hover:border-line-strong disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={
                  submitting ||
                  inputs.field_count === 0 ||
                  (isPlatformForm && !platformId)
                }
                className="flex items-center gap-1.5 rounded-sm border-2 border-accent bg-accent px-4 py-2 text-sm font-bold text-on-accent transition-colors hover:bg-accent-hover disabled:opacity-50"
              >
                {submitting && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                {mode === "edit" ? "Save changes" : "Create entry"}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── Tiny shared controls ────────────────────────────────────────────────

function NumberField({
  label,
  value,
  min = 0,
  max = 100,
  onChange,
}: {
  label: string;
  value: number;
  min?: number;
  max?: number;
  onChange: (v: number) => void;
}) {
  return (
    <div>
      <label className="label mb-1 block text-[10px]">{label}</label>
      <input
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(e) => {
          const n = parseInt(e.target.value || "0", 10);
          if (!Number.isNaN(n)) onChange(Math.max(min, Math.min(max, n)));
        }}
        className={cn(INPUT_CLS, "tnum font-mono")}
      />
    </div>
  );
}

function Checkbox({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <label className="flex cursor-pointer items-center gap-2">
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="h-3.5 w-3.5 rounded-xs border-line accent-[var(--accent)]"
      />
      <span className="text-sm text-fg-secondary">{label}</span>
    </label>
  );
}
