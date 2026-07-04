"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { AlertCircle, Eye, EyeOff, Settings } from "lucide-react";
import { api, type OrphanProject } from "@/lib/api";
import { Card } from "@/components/card";
import { Label, CodeChip, StatusPill } from "@/components/ui";
import { formatCurrency, formatNumber, platformLabel, cn } from "@/lib/utils";

/**
 * OrphanPanel — Flightdeck widget listing project_codes with spend/activity
 * in fact_* tables that don't have a dim_projects row.
 *
 * The only action here is Configure → redirect to
 * /admin/projects/new?code=XXXXX (form prefill).
 *
 * Suppression is deliberately NOT a UI action. To set a code aside you add a
 * row to the `dismissed_orphans` control table in BigQuery (level = 'dismissed'
 * hides it from the active panel but keeps it under "Show dismissed";
 * level = 'archived' hides it everywhere). That way nothing can be suppressed
 * by accident. The panel collapses itself when there's nothing to show.
 */
export function OrphanPanel() {
  const [orphans, setOrphans] = useState<OrphanProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDismissed, setShowDismissed] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const load = async (includeDismissed: boolean) => {
    setLoading(true);
    try {
      const data = await api.orphans.list(includeDismissed);
      setOrphans(data.orphans);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load orphans");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load(showDismissed);
  }, [showDismissed]);

  // Hide the entire panel when there's nothing to show.
  const hasOrphans = orphans.length > 0;
  if (!loading && !hasOrphans && !showDismissed && !error) {
    return null;
  }

  const topSpend = orphans.length
    ? Math.max(...orphans.map((o) => o.total_spend))
    : 0;

  return (
    <div className="mt-9">
      <div className="flex items-center gap-3">
        <AlertCircle className="h-4 w-4 text-fg-muted" />
        <Label className="text-fg-secondary">Unmapped Spend</Label>
        {hasOrphans && (
          <span className="rounded-pill bg-surface-sunken px-2 py-0.5 font-mono text-[11px] text-fg-muted">
            {orphans.length}
          </span>
        )}
        <div className="h-px flex-1 bg-line-soft" />
        {hasOrphans && !loading && (
          <button
            onClick={() => setExpanded((v) => !v)}
            className="flex items-center gap-1.5 text-xs text-fg-muted transition-colors hover:text-fg"
          >
            {expanded ? (
              <>
                <EyeOff className="h-3.5 w-3.5" />
                Hide
              </>
            ) : (
              <>
                <Eye className="h-3.5 w-3.5" />
                Show all
              </>
            )}
          </button>
        )}
        <button
          onClick={() => setShowDismissed((v) => !v)}
          className="flex items-center gap-1.5 text-xs text-fg-muted transition-colors hover:text-fg"
        >
          {showDismissed ? (
            <>
              <EyeOff className="h-3.5 w-3.5" />
              Hide dismissed
            </>
          ) : (
            <>
              <Eye className="h-3.5 w-3.5" />
              Show dismissed
            </>
          )}
        </button>
      </div>

      <p className="mt-2 text-xs text-fg-muted">
        Spend under these project codes isn&apos;t mapped to a live ADA
        project, so it is not part of your active book and nothing here is
        waiting on you. An admin maps a code by configuring it, or sets it
        aside in the dismissed_orphans control table. Dismissed codes stay
        hidden here but remain under &ldquo;Show dismissed,&rdquo; while
        archived codes are hidden everywhere.
      </p>

      {error && (
        <div className="mt-3 rounded-md border-2 border-tint-danger bg-tint-danger p-3 text-sm text-danger">
          {error}
        </div>
      )}

      {loading ? (
        <p className="mt-3 text-xs text-fg-muted">
          Checking for unmapped spend…
        </p>
      ) : !hasOrphans ? (
        <p className="mt-3 text-sm text-fg-muted">
          {showDismissed ? "No dismissed orphans." : "No unmapped spend detected."}
        </p>
      ) : !expanded ? (
        <p className="mt-3 text-xs text-fg-muted">
          {orphans.length} unmapped code{orphans.length === 1 ? "" : "s"} ·{" "}
          {formatCurrency(topSpend)} largest
        </p>
      ) : null}

      {expanded && hasOrphans && (
        <div className="mt-3 grid items-stretch gap-3 lg:grid-cols-2 xl:grid-cols-3">
          {orphans.map((o) => (
            <OrphanCard key={o.project_code} orphan={o} />
          ))}
        </div>
      )}
    </div>
  );
}

interface OrphanCardProps {
  orphan: OrphanProject;
}

function OrphanCard({ orphan: o }: OrphanCardProps) {
  const topPlatforms = o.by_platform.slice(0, 4);
  const extraPlatforms = o.by_platform.length - topPlatforms.length;

  return (
    <Card
      className={cn(
        "flex h-full flex-col transition-colors",
        o.dismissed && "opacity-60",
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <CodeChip>{o.project_code}</CodeChip>
            {o.dismissed ? (
              <StatusPill label="Dismissed" color="var(--done)" size="sm" dot={false} />
            ) : (
              <StatusPill label="Unmapped" color="var(--fg-muted)" size="sm" />
            )}
          </div>
          <p className="tnum mt-2 text-lg font-bold text-fg">
            {formatCurrency(o.total_spend)}
          </p>
          <p className="text-xs text-fg-muted">
            {formatNumber(o.total_rows)} rows
            {o.first_date && o.last_date && (
              <>
                {" · "}
                {o.first_date}
                {o.first_date !== o.last_date && ` → ${o.last_date}`}
              </>
            )}
          </p>
        </div>
      </div>

      {/* Platform breakdown */}
      <div className="mt-3 space-y-1.5">
        {topPlatforms.map((p) => (
          <div
            key={p.platform_id}
            className="flex items-center justify-between text-xs"
          >
            <span className="text-fg-secondary">
              {platformLabel(p.platform_id)}
            </span>
            <span className="tnum font-mono text-fg-muted">
              {formatCurrency(p.spend)}
            </span>
          </div>
        ))}
        {extraPlatforms > 0 && (
          <p className="text-xs text-fg-faint">
            + {extraPlatforms} more platform{extraPlatforms > 1 ? "s" : ""}
          </p>
        )}
      </div>

      {o.dismissed && o.dismissed_reason && (
        <div className="mt-3 rounded-sm border border-line-soft bg-surface-sunken p-2 text-xs text-fg-muted">
          <span className="font-semibold text-fg-secondary">Reason:</span>{" "}
          {o.dismissed_reason}
        </div>
      )}

      {/* The only action is Configure. Suppression is managed by an admin. */}
      {!o.dismissed && (
        <div className="mt-auto flex items-center gap-2 pt-4">
          <Link
            href={`/admin/projects/new?code=${o.project_code}`}
            className="flex flex-1 items-center justify-center gap-1.5 rounded-sm border-2 border-tint-ok bg-tint-ok px-3 py-2 text-xs font-bold text-ok transition-colors hover:opacity-80"
          >
            <Settings className="h-3.5 w-3.5" />
            Configure
          </Link>
        </div>
      )}
    </Card>
  );
}
