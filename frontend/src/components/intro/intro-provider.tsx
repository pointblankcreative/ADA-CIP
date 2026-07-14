"use client";

/**
 * IntroProvider — retained as a thin no-op context shell.
 *
 * The cold-load orbit-boot splash was removed (UAT #12): on live PROD it did
 * not dismiss and read as broken (still full-screen past a minute, no skip),
 * and three of four UAT testers first read it as a fault. This provider now
 * only satisfies the `useIntro().signalReady()` calls that pages still make in
 * their load `finally` blocks; those are harmless no-ops. Kept (rather than
 * deleted) so the two page imports keep compiling without touching them.
 */

import { createContext, useContext } from "react";

interface IntroApi {
  /** Legacy readiness hook; now a no-op (the splash was removed). */
  signalReady: () => void;
}

const IntroContext = createContext<IntroApi>({ signalReady: () => {} });

export const useIntro = () => useContext(IntroContext);

export function IntroProvider({ children }: { children: React.ReactNode }) {
  return (
    <IntroContext.Provider value={{ signalReady: () => {} }}>
      {children}
    </IntroContext.Provider>
  );
}
