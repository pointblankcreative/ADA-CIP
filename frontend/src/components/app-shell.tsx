"use client";

/**
 * AppShell — client wrapper owning the shared shell state: the top bar
 * and the ⌘K command palette. Children (the routed pages) pass through
 * as a server-rendered slot.
 */
import { useCallback, useEffect, useState } from "react";
import { TopBar } from "@/components/top-bar";
import { CommandPalette } from "@/components/command-palette";
import { IntroProvider } from "@/components/intro/intro-provider";

export function AppShell({ children }: { children: React.ReactNode }) {
  const [paletteOpen, setPaletteOpen] = useState(false);

  const openPalette = useCallback(() => setPaletteOpen(true), []);
  const closePalette = useCallback(() => setPaletteOpen(false), []);

  // ⌘K / Ctrl+K toggles the palette from anywhere.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        setPaletteOpen((p) => !p);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  return (
    <IntroProvider>
      <div className="flex min-h-screen flex-col">
        <TopBar onOpenPalette={openPalette} />
        <main className="min-w-0 flex-1">{children}</main>
        <CommandPalette open={paletteOpen} onClose={closePalette} />
      </div>
    </IntroProvider>
  );
}
