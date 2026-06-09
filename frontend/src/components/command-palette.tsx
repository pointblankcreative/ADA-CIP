"use client";

/**
 * ⌘K command palette — v0.3 Flightdeck shell.
 *
 * Replaces the sidebar's campaign quick-list: jump to any campaign or
 * page from anywhere. Campaigns load from the existing /api/projects/
 * endpoint on first open; filtering and keyboard navigation are local.
 */
import {
  Fragment,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useRouter } from "next/navigation";
import {
  Radar,
  AlertTriangle,
  Shield,
  Search,
  CornerDownLeft,
  Activity,
  type LucideIcon,
} from "lucide-react";
import { api, type Project } from "@/lib/api";
import { computeFlight, flightDotColor } from "@/lib/flight";
import { formatPercent, cn } from "@/lib/utils";

const API_DOCS_URL = `${process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"}/docs`;

interface NavItem {
  kind: "nav";
  id: string;
  label: string;
  sub: string;
  icon: LucideIcon;
  run: () => void;
}

interface CampaignItem {
  kind: "campaign";
  id: string;
  label: string;
  sub: string;
  dot: string;
  status: string;
  run: () => void;
}

type PaletteItem = NavItem | CampaignItem;

export function CommandPalette({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(0);
  const [projects, setProjects] = useState<Project[] | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Load the campaign list on first open; reuse it afterwards.
  useEffect(() => {
    if (!open || projects !== null) return;
    api.projects
      .list()
      .then(setProjects)
      .catch(() => setProjects([]));
  }, [open, projects]);

  useEffect(() => {
    if (open) {
      setQuery("");
      setSelected(0);
      const t = setTimeout(() => inputRef.current?.focus(), 30);
      return () => clearTimeout(t);
    }
  }, [open]);

  useEffect(() => {
    setSelected(0);
  }, [query]);

  const items = useMemo<PaletteItem[]>(() => {
    const nav: NavItem[] = [
      {
        kind: "nav",
        id: "flightdeck",
        label: "Flightdeck",
        sub: "The board",
        icon: Radar,
        run: () => router.push("/"),
      },
      {
        kind: "nav",
        id: "alerts",
        label: "Alerts",
        sub: "Active warnings",
        icon: AlertTriangle,
        run: () => router.push("/alerts"),
      },
      {
        kind: "nav",
        id: "admin",
        label: "Admin",
        sub: "Projects, pipeline & sync",
        icon: Shield,
        run: () => router.push("/admin"),
      },
      {
        kind: "nav",
        id: "api-docs",
        label: "API Docs",
        sub: "Backend OpenAPI reference",
        icon: Activity,
        run: () => window.open(API_DOCS_URL, "_blank", "noopener,noreferrer"),
      },
    ];
    const camps: CampaignItem[] = (projects ?? []).map((p) => {
      const f = computeFlight(p);
      return {
        kind: "campaign",
        id: p.project_code,
        label: p.project_name,
        sub: `${p.project_code}${p.client_name ? ` · ${p.client_name}` : ""}`,
        dot: flightDotColor(p, f),
        status: f.ended
          ? "Ended"
          : f.noData
            ? "Awaiting"
            : formatPercent(p.pacing_percentage),
        run: () => router.push(`/project/${p.project_code}`),
      };
    });
    const all: PaletteItem[] = [...nav, ...camps];
    const needle = query.trim().toLowerCase();
    if (!needle) return all;
    return all.filter((it) =>
      `${it.label} ${it.sub}`.toLowerCase().includes(needle)
    );
  }, [projects, query, router]);

  if (!open) return null;

  const choose = (it: PaletteItem | undefined) => {
    if (!it) return;
    it.run();
    onClose();
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelected((s) => Math.min(s + 1, items.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelected((s) => Math.max(s - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      choose(items[selected]);
    } else if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    }
  };

  let lastKind: string | null = null;

  return (
    <div
      onClick={onClose}
      className="fixed inset-0 z-[200] flex items-start justify-center px-5 pb-5 pt-[12vh] animate-fade-up"
      style={{
        background: "color-mix(in srgb, var(--dark-900) 62%, transparent)",
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-[560px] overflow-hidden rounded-lg border-2 border-line bg-surface-card shadow-soft"
      >
        <div className="flex items-center gap-3 border-b-2 border-line-soft px-[18px] py-4">
          <Search className="h-[18px] w-[18px] text-fg-faint" />
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Jump to a campaign or page…"
            className="flex-1 bg-transparent text-base text-fg outline-none placeholder:text-fg-faint"
          />
          <kbd className="rounded-xs border border-line-soft bg-surface-sunken px-[7px] py-[3px] font-mono text-[10px] text-fg-faint">
            ESC
          </kbd>
        </div>
        <div className="max-h-[48vh] overflow-y-auto p-2">
          {items.length === 0 && (
            <div className="px-4 py-[26px] text-center text-[13px] text-fg-faint">
              {projects === null
                ? "Loading campaigns…"
                : `No matches for “${query}”.`}
            </div>
          )}
          {items.map((it, i) => {
            const showHead = it.kind !== lastKind;
            lastKind = it.kind;
            const active = i === selected;
            return (
              <Fragment key={`${it.kind}-${it.id}`}>
                {showHead && (
                  <div className="label px-3 pb-[5px] pt-2.5 text-[9px] text-fg-faint">
                    {it.kind === "nav" ? "Go to" : "Campaigns"}
                  </div>
                )}
                <button
                  onClick={() => choose(it)}
                  onMouseEnter={() => setSelected(i)}
                  className={cn(
                    "flex w-full items-center gap-3 rounded-sm px-3 py-2.5 text-left transition-colors duration-fast",
                    active ? "bg-tint-accent" : "bg-transparent"
                  )}
                >
                  {it.kind === "nav" ? (
                    <it.icon
                      className={cn(
                        "h-[17px] w-[17px] flex-shrink-0",
                        active ? "text-accent-ink" : "text-fg-muted"
                      )}
                    />
                  ) : (
                    <span
                      className="h-2 w-2 flex-shrink-0 rounded-full"
                      style={{ backgroundColor: it.dot }}
                    />
                  )}
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-sm font-semibold text-fg">
                      {it.label}
                    </span>
                    <span className="block truncate font-mono text-[10.5px] tracking-[0.03em] text-fg-meta">
                      {it.sub}
                    </span>
                  </span>
                  {it.kind === "campaign" && (
                    <span className="tnum flex-shrink-0 font-mono text-[11px] text-fg-faint">
                      {it.status}
                    </span>
                  )}
                  {active && (
                    <CornerDownLeft className="h-3.5 w-3.5 flex-shrink-0 text-fg-faint" />
                  )}
                </button>
              </Fragment>
            );
          })}
        </div>
      </div>
    </div>
  );
}
