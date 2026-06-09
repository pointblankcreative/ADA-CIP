/**
 * Form Friction Score (FFS) — client-side preview computation.
 *
 * Mirrors `backend/services/diagnostics/shared/form_friction.py:compute_ffs`
 * exactly. Any change here MUST be reflected there and vice versa.
 * The server recomputes authoritatively on every write, so this is purely
 * for the live preview in the wizard.
 */

import type { FFSInputs } from "@/lib/api";

// ── Field type friction weights (matches backend FIELD_TYPE_FRICTION) ──

export const FIELD_TYPE_FRICTION: Record<string, number> = {
  text_name: 1,
  text_email: 1,
  text_phone: 3,
  text_address: 5,
  text_freeform: 4,
  dropdown_simple: 2, // <5 options
  dropdown_complex: 3, // 5+ options
  radio: 1,
  checkbox: 1,
  file_upload: 8,
  date_picker: 3,
  multi_step: 5, // per additional step
  captcha: 2,
};

export const FIELD_TYPE_LABELS: Record<string, string> = {
  text_name: "Name",
  text_email: "Email",
  text_phone: "Phone",
  text_address: "Address",
  text_freeform: "Freeform text",
  dropdown_simple: "Dropdown (<5 options)",
  dropdown_complex: "Dropdown (5+ options)",
  radio: "Radio",
  checkbox: "Checkbox",
  file_upload: "File upload",
  date_picker: "Date picker",
  multi_step: "Multi-step",
  captcha: "CAPTCHA",
};

/** Compute FFS 0–100 from wizard inputs. Mirrors backend compute_ffs. */
export function computeFFS(inputs: FFSInputs): number {
  let score = 0;

  // 1. Field count (0–30)
  const fc = inputs.field_count ?? 0;
  if (fc <= 3) score += 5;
  else if (fc <= 6) score += 10;
  else if (fc <= 10) score += 18;
  else if (fc <= 15) score += 24;
  else score += 30;

  // 2. Required fields ratio (0–15)
  const req = inputs.required_fields ?? fc;
  if (fc > 0) {
    const ratio = req / fc;
    score += ratio * 15;
  }

  // 3. Field type friction (0–20)
  const types = inputs.field_types ?? [];
  const typeFriction = types.reduce(
    (acc, ft) => acc + (FIELD_TYPE_FRICTION[ft] ?? 2),
    0
  );
  score += Math.min(typeFriction / 2, 20);

  // 4. Clicks to submit (0–10)
  const clicks = inputs.clicks_to_submit ?? 1;
  score += Math.min(clicks * 2.5, 10);

  // 5. Below-fold mobile (0–15)
  if (inputs.below_fold_mobile) score += 15;

  // 6. Autofill discount (−5)
  if (inputs.has_autofill) score -= 5;

  // 7. Platform-form discount (−5)
  if (inputs.is_platform_form) score -= 5;

  return Math.max(0, Math.min(100, Math.round(score * 10) / 10));
}

/** Bucket a score into a severity label + colour class (design-system tokens). */
export function ffsBucket(score: number): {
  label: string;
  color: string;
  bg: string;
  /** Tinted border class — pairs with a `border` width utility. */
  ring: string;
  fill: string;
} {
  if (score < 20) {
    return {
      label: "Low friction",
      color: "text-ok",
      bg: "bg-tint-ok",
      ring: "border-tint-ok",
      fill: "bg-ok",
    };
  }
  if (score < 50) {
    return {
      label: "Moderate friction",
      color: "text-warn",
      bg: "bg-tint-warn",
      ring: "border-tint-warn",
      fill: "bg-warn",
    };
  }
  return {
    label: "High friction",
    color: "text-danger",
    bg: "bg-tint-danger",
    ring: "border-tint-danger",
    fill: "bg-danger",
  };
}

export const DEFAULT_FFS_INPUTS: FFSInputs = {
  field_count: 0,
  required_fields: 0,
  field_types: [],
  clicks_to_submit: 1,
  below_fold_mobile: false,
  has_autofill: false,
  is_platform_form: false,
};
