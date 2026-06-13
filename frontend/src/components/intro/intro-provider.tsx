"use client";

/**
 * IntroProvider — owns the cold-load splash and the readiness handshake.
 *
 * Mounted once inside AppShell, so it runs on a full page load (and hard
 * reload) but NOT on client-side route changes — the splash is the cold-boot
 * screen, not a per-navigation interstitial.
 *
 * The contract: whichever page loads first calls `useIntro().signalReady()`
 * when its primary data has resolved (the Flightdeck's project list, or a
 * deep-linked project's detail). That raises `dataReady`, which lets the
 * splash run its green pops and reveal — so we never lift the curtain on an
 * empty screen. A hard ceiling (REVEAL_BY_MS, mirrored in OrbitIntro) forces
 * completion if a fetch hangs or errors, and `signalReady` after the splash
 * has finished is a harmless no-op.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { OrbitIntro } from "./orbit-intro";

const REVEAL_BY_MS = 9000;

interface IntroApi {
  /** Raise app-readiness so the splash can pop green and reveal. */
  signalReady: () => void;
}

const IntroContext = createContext<IntroApi>({ signalReady: () => {} });

export const useIntro = () => useContext(IntroContext);

export function IntroProvider({ children }: { children: React.ReactNode }) {
  const [visible, setVisible] = useState(true);
  const [dataReady, setDataReady] = useState(false);
  const doneRef = useRef(false);

  const signalReady = useCallback(() => setDataReady(true), []);

  // Hard ceiling: complete even if nothing ever signals readiness.
  useEffect(() => {
    const id = window.setTimeout(() => setDataReady(true), REVEAL_BY_MS);
    return () => window.clearTimeout(id);
  }, []);

  // Optional debug/manual bridge (e.g. window.adaIntro.ready() in devtools).
  useEffect(() => {
    (window as unknown as { adaIntro?: IntroApi }).adaIntro = { signalReady };
  }, [signalReady]);

  return (
    <IntroContext.Provider value={{ signalReady }}>
      {children}
      {visible && (
        <OrbitIntro
          dataReady={dataReady}
          onFinished={() => {
            if (doneRef.current) return;
            doneRef.current = true;
            setVisible(false);
          }}
        />
      )}
    </IntroContext.Provider>
  );
}
