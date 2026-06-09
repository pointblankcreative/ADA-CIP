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
  /* data-theme="dark" pins the whole app dark while legacy slate-styled
     screens are migrated to tokens. The light default (:root) becomes the
     live theme when this attribute is removed — screens rebuilt on tokens
     are theme-agnostic and need no further changes. */
  return (
    <html lang="en" data-theme="dark">
      <body className={`${fontVariables} min-h-screen font-sans`}>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
