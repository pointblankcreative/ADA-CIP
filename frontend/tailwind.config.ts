import type { Config } from "tailwindcss";

/**
 * ADA-CIP Tailwind theme — Point Blank design system.
 *
 * Every semantic colour maps to a CSS custom property defined in
 * globals.css, so light/dark is an attribute flip ([data-theme="dark"])
 * and components never hardcode hex. Status tints (13% bg / 35% border)
 * live as .bg-tint-* / .border-tint-* utilities in globals.css because
 * Tailwind alpha modifiers don't compose with var() colours.
 *
 * The `brand` (blue) scale is legacy-only: it keeps un-migrated slate
 * screens rendering during the re-skin and is deleted in the final sweep.
 */
const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        /* ---- Surfaces ---- */
        surface: {
          DEFAULT: "var(--surface-page)", // legacy `bg-surface` ≈ page
          page: "var(--surface-page)",
          card: "var(--surface-card)",
          raised: "var(--surface-card)", // legacy `bg-surface-raised` = cards
          up: "var(--surface-raised)", // raised-above-card (hover, popovers)
          sunken: "var(--surface-sunken)",
          inverse: "var(--surface-inverse)",
        },
        /* ---- Text ---- */
        fg: {
          DEFAULT: "var(--text-primary)",
          secondary: "var(--text-secondary)",
          muted: "var(--text-muted)",
          faint: "var(--text-faint)",
          meta: "var(--text-meta)",
          inverse: "var(--text-inverse)",
        },
        /* ---- Borders ---- */
        line: {
          DEFAULT: "var(--border)",
          soft: "var(--border-soft)",
          strong: "var(--border-strong)",
        },
        /* ---- Accent (chartreuse) ---- */
        accent: {
          DEFAULT: "var(--accent)",
          ink: "var(--accent-ink)", // text-safe accent (darkened on light)
          hover: "var(--accent-hover)",
          press: "var(--accent-press)",
        },
        "on-accent": "var(--on-accent)",
        /* ---- Status ---- */
        ok: "var(--ok)",
        warn: "var(--warn)",
        danger: "var(--danger)",
        info: "var(--info)",
        done: "var(--done)",
        /* ---- Raw brand (rare direct use) ---- */
        pb: {
          dark: "var(--pb-dark)",
          chartreuse: "var(--pb-chartreuse)",
          light: "var(--pb-light)",
          white: "var(--pb-white)",
        },
        /* ---- LEGACY — delete in final sweep ---- */
        brand: {
          50: "#eff6ff",
          100: "#dbeafe",
          200: "#bfdbfe",
          300: "#93c5fd",
          400: "#60a5fa",
          500: "#3b82f6",
          600: "#2563eb",
          700: "#1d4ed8",
          800: "#1e40af",
          900: "#1e3a8a",
          950: "#172554",
        },
      },
      fontFamily: {
        sans: ["var(--font-body)"],
        mono: ["var(--font-mono)"],
        display: ["var(--font-display)"],
        script: ["var(--font-script)"],
      },
      /* Radii — deliberately tight. The asterisk is sharp; so are we. */
      borderRadius: {
        none: "0",
        xs: "2px",
        sm: "4px",
        DEFAULT: "4px",
        md: "6px",
        lg: "10px",
        xl: "10px",
        pill: "999px",
        full: "9999px",
      },
      boxShadow: {
        hard: "var(--shadow-hard)",
        "hard-accent": "var(--shadow-hard-accent)",
        soft: "var(--shadow-soft)",
      },
      transitionTimingFunction: {
        snap: "cubic-bezier(0.2, 0, 0, 1)",
      },
      transitionDuration: {
        fast: "120ms",
        base: "180ms",
        slow: "280ms",
      },
      letterSpacing: {
        meta: "0.14em",
        "meta-lg": "0.22em",
      },
      keyframes: {
        "fade-up": {
          from: { opacity: "0", transform: "translateY(6px)" },
          to: { opacity: "1", transform: "none" },
        },
      },
      animation: {
        "fade-up": "fade-up 0.36s cubic-bezier(0.2, 0, 0, 1) both",
      },
    },
  },
  plugins: [],
};

export default config;
