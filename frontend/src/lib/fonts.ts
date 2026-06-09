/**
 * ADA-CIP brand fonts, self-hosted via next/font/local.
 *
 * Sourced from the Point Blank design system export (_ds/…/fonts). Each
 * exposes a CSS variable that globals.css composes into the semantic
 * font tokens (--font-display / --font-body / --font-mono / --font-script)
 * with their fallback stacks.
 *
 * - Folsom Black  — PRIMARY display. ALL CAPS, short statements.
 * - Inter         — all body copy (variable 100–900 + italic).
 * - Chivo Mono    — functional details: labels, numbers, metadata.
 * - Des Montilles — expressive script. Sparing use only.
 */
import localFont from "next/font/local";

export const folsom = localFont({
  src: "../fonts/folsom-black-web.woff2",
  weight: "900",
  style: "normal",
  display: "swap",
  variable: "--font-folsom",
});

export const inter = localFont({
  src: [
    {
      path: "../fonts/Inter-Variable.ttf",
      weight: "100 900",
      style: "normal",
    },
    {
      path: "../fonts/Inter-Italic-Variable.ttf",
      weight: "100 900",
      style: "italic",
    },
  ],
  display: "swap",
  variable: "--font-inter",
});

export const chivoMono = localFont({
  src: "../fonts/ChivoMono-Variable.ttf",
  weight: "100 900",
  style: "normal",
  display: "swap",
  variable: "--font-chivo",
});

export const desMontilles = localFont({
  src: "../fonts/DesMontilles-Regular.otf",
  weight: "400",
  style: "normal",
  display: "swap",
  variable: "--font-desmontilles",
});

/** Convenience: all four variable classes for the <body> element. */
export const fontVariables = [
  folsom.variable,
  inter.variable,
  chivoMono.variable,
  desMontilles.variable,
].join(" ");
