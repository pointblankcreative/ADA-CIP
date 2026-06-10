"use client";

/**
 * ADA-CIP top app bar — v0.3 Flightdeck shell.
 *
 * Replaces the persistent sidebar: brand lockup, three nav destinations
 * (Flightdeck / Alerts / Admin), the ⌘K "Jump to campaign" affordance,
 * and the Des Montilles sign-off. The campaign quick-list the sidebar
 * used to carry lives in the command palette now.
 */
import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Radar, AlertTriangle, Shield, Search } from "lucide-react";
import { api } from "@/lib/api";
import { PBMark } from "@/components/ui";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/", label: "Flightdeck", icon: Radar },
  { href: "/alerts", label: "Alerts", icon: AlertTriangle },
  { href: "/admin", label: "Admin", icon: Shield },
] as const;

function isActive(href: string, pathname: string): boolean {
  if (href === "/") return pathname === "/" || pathname.startsWith("/project");
  return pathname.startsWith(href);
}

export function TopBar({ onOpenPalette }: { onOpenPalette: () => void }) {
  const pathname = usePathname();
  const [criticalCount, setCriticalCount] = useState(0);
  const [kbdHint, setKbdHint] = useState("⌘K");

  useEffect(() => {
    api.alerts
      .list({ severity: "critical", limit: 100 })
      .then((alerts) =>
        setCriticalCount(alerts.filter((a) => !a.acknowledged_at).length)
      )
      .catch(() => setCriticalCount(0));
  }, []);

  useEffect(() => {
    const mac = /Mac|iPhone|iPad/.test(navigator.platform ?? "");
    setKbdHint(mac ? "⌘K" : "Ctrl K");
  }, []);

  return (
    <header className="sticky top-0 z-40 flex h-[58px] flex-shrink-0 items-center gap-3 border-b-2 border-line-soft bg-surface-sunken px-4 sm:gap-5 sm:px-6">
      {/* brand lockup */}
      <Link href="/" className="flex items-center gap-2.5">
        <PBMark size={19} />
        <span className="text-left leading-none">
          <span className="display block text-[19px] leading-[0.85] text-fg">
            ADA
          </span>
          <span className="mt-px block font-mono text-[7.5px] uppercase tracking-[0.18em] text-fg-meta">
            Campaign Intel
          </span>
        </span>
      </Link>

      <div className="hidden h-[26px] w-px bg-line-soft sm:block" />

      {/* nav */}
      <nav className="flex gap-0.5">
        {NAV.map(({ href, label, icon: Icon }) => {
          const active = isActive(href, pathname);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "inline-flex items-center gap-2 rounded-sm px-2.5 py-2 text-[13.5px] transition-all duration-fast sm:px-[13px]",
                active
                  ? "bg-tint-accent font-semibold text-accent-ink"
                  : "font-medium text-fg-secondary hover:bg-surface-card hover:text-fg"
              )}
            >
              <Icon className="h-4 w-4 flex-shrink-0" />
              <span className="hidden sm:inline">{label}</span>
              {href === "/alerts" && criticalCount > 0 && (
                <span className="rounded-pill bg-tint-danger px-1.5 font-mono text-[10px] font-bold text-danger">
                  {criticalCount}
                </span>
              )}
            </Link>
          );
        })}
      </nav>

      {/* jump to campaign */}
      <button
        onClick={onOpenPalette}
        className="ml-auto inline-flex items-center gap-2 rounded-sm border-2 border-line-soft bg-surface-card px-2.5 py-[7px] text-fg-faint transition-colors duration-fast hover:border-line sm:gap-2.5 sm:px-3"
      >
        <Search className="h-[15px] w-[15px]" />
        <span className="hidden text-xs text-fg-muted md:inline">
          Jump to campaign
        </span>
        <kbd className="hidden rounded-xs border border-line-soft bg-surface-sunken px-1.5 py-0.5 font-mono text-[10px] tracking-[0.04em] text-fg-faint sm:inline">
          {kbdHint}
        </kbd>
      </button>

      {/* sign-off */}
      <span className="script hidden text-lg leading-none text-accent-ink lg:block">
        Loud on purpose.
      </span>
    </header>
  );
}
