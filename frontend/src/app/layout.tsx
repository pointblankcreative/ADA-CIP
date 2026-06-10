import type { Metadata } from "next";
import { AppShell } from "@/components/app-shell";
import { fontVariables } from "@/lib/fonts";
import "./globals.css";

export const metadata: Metadata = {
  title: "ADA — Campaign Intelligence Platform",
  description:
    "Campaign monitoring, budget pacing, and automated reporting for Point Blank",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  /* Light is the live theme (:root tokens). Every screen renders on
     semantic tokens, so dark mode is a data-theme="dark" attribute flip
     on <html> whenever we want it.

     The next/font variable classes MUST sit on <html>, not <body>: the
     semantic font tokens (--font-display etc.) are composed in :root and
     custom properties resolve their var() references at the element that
     DEFINES them. With the classes on <body>, --font-folsom didn't exist
     at :root, the composed tokens computed to invalid-empty, and every
     font-family fell through to the UA serif. */
  return (
    <html lang="en" className={fontVariables}>
      <body className="min-h-screen font-sans">
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
