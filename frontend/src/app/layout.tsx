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
     on <html> whenever we want it. */
  return (
    <html lang="en">
      <body className={`${fontVariables} min-h-screen font-sans`}>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
