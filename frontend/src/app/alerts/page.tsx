"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertTriangle,
  CheckCircle2,
  Clock,
  Info,
  OctagonAlert,
  RefreshCw,
} from "lucide-react";
import { api, type Alert } from "@/lib/api";
import { Card } from "@/components/card";
import { Btn, CodeChip, Eyebrow } from "@/components/ui";
import { cn, severityVar } from "@/lib/utils";
import { formatAlertSource } from "@/lib/alert-labels";

const FILTERS: Array<[string, string]> = [
  ["", "All"],
  ["critical", "Critical"],
  ["warning", "Warning"],
  ["info", "Info"],
];

function filterColor(sev: string): string {
  if (sev === "critical") return "var(--danger)";
  if (sev === "warning") return "var(--warn)";
  if (sev === "info") return "var(--info)";
  return "var(--accent-ink)";
}

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [severity, setSeverity] = useState<string>("");
  // Standing count of signals in the diagnostic "Act now" band across active
  // campaigns, sourced from the SAME definition the Diagnostics board uses
  // (guard-passed signals with status ACTION) so the two surfaces agree.
  // null = unknown / not yet loaded / fetch failed -> the cards fall back to
  // the no-number wording. 0 is a real value. Fetched independently of the
  // alert feed so the feed (which can 500) never blocks or masks it.
  const [actNow, setActNow] = useState<number | null>(null);
  const [actNowLink, setActNowLink] = useState<string>("/");

  const load = async () => {
    setLoading(true);
    setError(false);
    try {
      const data = await api.alerts.list({
        severity: severity || undefined,
        limit: 100,
      });
      setAlerts(data);
    } catch {
      // The feed endpoint can fail (a 500 currently masks itself as empty).
      // Record the error so the UI shows an honest "did not load" state
      // instead of a falsely reassuring empty/all-clear state.
      setAlerts([]);
      setError(true);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [severity]);

  // Act-now count: read-only, 1 + N(active) calls, fully resilient. Mirrors
  // the board's act-band membership exactly. Any failure leaves actNow null
  // and the empty/error cards render the no-number copy. Runs once.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const projects = await api.projects.list();
        const active = projects.filter(
          (p) => p.status === "active" && !p.recently_ended
        );
        if (active.length === 0) {
          if (!cancelled) {
            setActNow(0);
            setActNowLink("/");
          }
          return;
        }
        const results = await Promise.allSettled(
          active.map((p) => api.diagnostics.get(p.project_code))
        );
        let anyOk = false;
        let count = 0;
        for (const r of results) {
          if (r.status !== "fulfilled") continue;
          anyOk = true;
          for (const out of r.value ?? []) {
            for (const sig of out.signals ?? []) {
              if (sig.guard_passed && sig.status === "ACTION") count += 1;
            }
          }
        }
        if (!cancelled) {
          // Every diagnostics call failed -> unknown (null), not a false 0.
          setActNow(anyOk ? count : null);
          setActNowLink(
            active.length === 1
              ? `/project/${active[0].project_code}`
              : "/"
          );
        }
      } catch {
        if (!cancelled) setActNow(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  /** Acknowledge with optional action note; the backend records the IAP
   *  user and we mirror its response into local state. */
  const handleAcknowledge = async (alertId: string, note?: string) => {
    const res = await api.alerts.acknowledge(alertId, note);
    setAlerts((prev) =>
      prev.map((a) =>
        a.alert_id === alertId
          ? {
              ...a,
              acknowledged_at: new Date().toISOString(),
              acknowledged_by: res.acknowledged_by,
              ack_note: res.ack_note,
            }
          : a
      )
    );
  };

  const criticalCount = alerts.filter((a) => a.severity === "critical").length;
  const warningCount = alerts.filter((a) => a.severity === "warning").length;
  const infoCount = alerts.filter((a) => a.severity === "info").length;

  return (
    <div className="mx-auto max-w-[980px] px-5 pb-20 pt-7 sm:px-7">
      <div className="flex flex-wrap items-end justify-between gap-5">
        <div>
          <Eyebrow>Pacing · health · data integrity</Eyebrow>
          <h1 className="display mt-2.5 text-[38px] text-fg sm:text-[44px]">
            Alerts
          </h1>
          <p className="mt-3 text-sm text-fg-muted">
            {loading
              ? "Loading the latest alerts."
              : error
                ? "The feed did not load, so this is not an all-clear."
                : alerts.length === 0
                  ? "Nothing new in the feed. A heads-up channel, not a full health check."
                  : `${alertSummary(criticalCount, warningCount, infoCount)}. Newest first.`}
          </p>
        </div>
        <Btn
          variant="outline"
          size="sm"
          onClick={load}
          disabled={loading}
          icon={<RefreshCw className={cn("h-3.5 w-3.5", loading && "animate-spin")} />}
        >
          Refresh
        </Btn>
      </div>

      {/* Filters */}
      <div className="mt-6 flex flex-wrap items-center gap-2">
        {FILTERS.map(([sev, label]) => {
          const active = severity === sev;
          const c = filterColor(sev);
          return (
            <button
              key={sev}
              onClick={() => setSeverity(sev)}
              className="rounded-pill px-3.5 py-[7px] font-mono text-[11.5px] font-semibold uppercase tracking-[0.06em] transition-all duration-fast"
              style={
                active
                  ? {
                      color: c,
                      border: `2px solid ${c}`,
                      backgroundColor: `color-mix(in srgb, ${c} 14%, transparent)`,
                    }
                  : {
                      color: "var(--text-secondary)",
                      border: "2px solid var(--border)",
                      backgroundColor: "transparent",
                    }
              }
            >
              {label}
            </button>
          );
        })}
      </div>

      {/* Alert list */}
      <div className="mt-5 space-y-2.5">
        {loading ? (
          Array.from({ length: 5 }).map((_, i) => (
            <Card key={i} className="animate-pulse">
              <div className="h-4 w-48 rounded bg-surface-sunken" />
              <div className="mt-2 h-3 w-72 rounded bg-surface-sunken" />
            </Card>
          ))
        ) : error ? (
          <FeedErrorCard onRefresh={load} actNow={actNow} actNowLink={actNowLink} />
        ) : alerts.length === 0 ? (
          <FeedEmptyCard actNow={actNow} actNowLink={actNowLink} />
        ) : (
          alerts.map((alert) => (
            <AlertRow
              key={alert.alert_id}
              alert={alert}
              onAcknowledge={handleAcknowledge}
            />
          ))
        )}
      </div>
    </div>
  );
}

/** Compact, count-driven feed summary. Never says "All clear". */
function alertSummary(critical: number, warning: number, info: number): string {
  const parts: string[] = [];
  if (critical > 0) parts.push(`${critical} critical`);
  if (warning > 0) parts.push(`${warning} warning`);
  if (info > 0) parts.push(`${info} info`);
  return parts.length > 0 ? parts.join(", ") : "Alerts in the feed";
}

/** Shared "open the board" link. */
function BoardLink({ href, label }: { href: string; label: string }) {
  return (
    <Link href={href} className="underline underline-offset-2 hover:opacity-70">
      {label}
    </Link>
  );
}

/** Genuinely-empty feed. Honest heads-up framing, no green "all-clear"
 *  check, and (when known) the live Act-now count from the board. */
function FeedEmptyCard({
  actNow,
  actNowLink,
}: {
  actNow: number | null;
  actNowLink: string;
}) {
  return (
    <Card className="flex flex-col items-center py-12 text-center">
      <Info className="h-9 w-9 text-fg-faint opacity-70" />
      <p className="mt-3 font-medium text-fg">Nothing new to flag right now.</p>
      <p className="mt-2 max-w-[42rem] text-sm leading-relaxed text-fg-muted">
        Alerts flag new or changed events as they happen, deduped daily, so this
        feed is a heads-up channel rather than a full health check.{" "}
        {actNow == null ? (
          <>
            For where each campaign stands now, including anything in the Act now
            band, <BoardLink href="/" label="open its Diagnostics board" />.
          </>
        ) : actNow === 0 ? (
          <>
            Diagnostics currently shows no signals in the Act now band.{" "}
            <BoardLink href={actNowLink} label="Open the board" /> for the full
            picture, including anything to keep an eye on.
          </>
        ) : (
          <>
            Diagnostics currently shows {actNow} signal{actNow === 1 ? "" : "s"}{" "}
            in the Act now band, so{" "}
            <BoardLink href={actNowLink} label="open the board" /> to see where{" "}
            {actNow === 1 ? "it stands" : "each one stands"}.
          </>
        )}
      </p>
    </Card>
  );
}

/** Feed failed to load. The previous behaviour swallowed the error and
 *  rendered the empty/all-clear state; this states plainly that it did not
 *  load and is NOT an all-clear, and still surfaces the board's Act-now
 *  count (it comes from a different, healthy endpoint). */
function FeedErrorCard({
  onRefresh,
  actNow,
  actNowLink,
}: {
  onRefresh: () => void;
  actNow: number | null;
  actNowLink: string;
}) {
  return (
    <Card className="flex flex-col items-center py-12 text-center">
      <AlertTriangle className="h-9 w-9 text-warn opacity-90" />
      <p className="mt-3 font-medium text-fg">The alert feed did not load.</p>
      <p className="mt-2 max-w-[42rem] text-sm leading-relaxed text-fg-muted">
        This is not an all-clear. Alerts could not be reached, so any that are
        active are not shown here.{" "}
        {actNow == null ? (
          <>
            Try refreshing, and check each campaign&apos;s{" "}
            <BoardLink href="/" label="Diagnostics board" /> for where it stands
            now.
          </>
        ) : actNow === 0 ? (
          <>
            Diagnostics did load and currently shows no signals in the Act now
            band. <BoardLink href={actNowLink} label="Open the board" /> for the
            full picture.
          </>
        ) : (
          <>
            Diagnostics did load and currently shows {actNow} signal
            {actNow === 1 ? "" : "s"} in the Act now band.{" "}
            <BoardLink href={actNowLink} label="Open the board" />.
          </>
        )}
      </p>
      <Btn
        variant="outline"
        size="sm"
        onClick={onRefresh}
        className="mt-4"
        icon={<RefreshCw className="h-3.5 w-3.5" />}
      >
        Refresh
      </Btn>
    </Card>
  );
}

function AlertRow({
  alert,
  onAcknowledge,
}: {
  alert: Alert;
  onAcknowledge: (alertId: string, note?: string) => Promise<void>;
}) {
  const [busy, setBusy] = useState(false);
  const [formOpen, setFormOpen] = useState(false);
  const [noteText, setNoteText] = useState("");
  const c = severityVar(alert.severity);
  const SeverityIcon =
    alert.severity === "critical"
      ? OctagonAlert
      : alert.severity === "warning"
        ? AlertTriangle
        : Info;

  const timeAgo = formatTimeAgo(alert.created_at);

  const ack = async () => {
    setBusy(true);
    try {
      await onAcknowledge(alert.alert_id, noteText || undefined);
      setFormOpen(false);
      setNoteText("");
    } catch {
      /* leave the form open */
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      className={cn(
        "rounded-md border-2 border-line-soft bg-surface-card px-4 py-[15px]",
        alert.acknowledged_at && "opacity-60"
      )}
      style={{ borderLeft: `3px solid ${c}` }}
    >
      <div className="flex items-start gap-3.5">
        <SeverityIcon
          className="mt-0.5 h-[17px] w-[17px] flex-shrink-0"
          style={{ color: c }}
        />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span
              className="font-mono text-[10.5px] font-semibold uppercase tracking-[0.08em]"
              style={{ color: c }}
            >
              {alert.title}
            </span>
            <span className="h-[3px] w-[3px] rounded-full bg-line" />
            <span className="font-mono text-[10px] uppercase tracking-[0.06em] text-fg-faint">
              {formatAlertSource(alert.alert_type)}
            </span>
            <span className="h-[3px] w-[3px] rounded-full bg-line" />
            <Link
              href={`/project/${alert.project_code}`}
              className="transition-opacity hover:opacity-70"
            >
              <CodeChip>{alert.project_code}</CodeChip>
            </Link>
          </div>
          <p className="mt-[7px] text-[13.5px] leading-normal text-fg-secondary">
            {alert.message}
          </p>
          {alert.acknowledged_at && (
            <span className="mt-1.5 inline-flex items-center gap-1 font-mono text-[10px] uppercase tracking-[0.06em] text-ok">
              <CheckCircle2 className="h-3 w-3" />
              Acknowledged
              {alert.acknowledged_by ? ` by ${alert.acknowledged_by}` : ""}
            </span>
          )}
          {alert.ack_note && (
            <p className="mt-1 text-xs italic text-fg-muted">
              “{alert.ack_note}”
            </p>
          )}
        </div>
        <div className="flex flex-shrink-0 flex-col items-end gap-2">
          <span className="inline-flex items-center gap-1 whitespace-nowrap font-mono text-[10.5px] text-fg-faint">
            <Clock className="h-3 w-3" />
            {timeAgo}
          </span>
          {!alert.acknowledged_at && !formOpen && (
            <button
              onClick={() => setFormOpen(true)}
              className="rounded-sm border-2 border-line px-2.5 py-1 font-mono text-[10px] font-semibold uppercase tracking-[0.06em] text-fg-muted transition-colors hover:border-line-strong hover:text-fg"
              title="Acknowledge this alert"
            >
              Acknowledge
            </button>
          )}
        </div>
      </div>
      {/* Acknowledge form: optional action note for the audit trail */}
      {!alert.acknowledged_at && formOpen && (
        <div className="mt-3 flex flex-wrap items-center gap-2 pl-[31px]">
          <input
            autoFocus
            value={noteText}
            onChange={(e) => setNoteText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") ack();
              if (e.key === "Escape") setFormOpen(false);
            }}
            maxLength={1000}
            placeholder="Optional — what did you do? e.g. lowered Meta daily caps, paused campaign"
            className="min-w-[260px] flex-1 rounded-sm border-2 border-line bg-surface-sunken px-2.5 py-1.5 text-xs text-fg placeholder:text-fg-faint outline-none focus:border-accent"
          />
          <button
            onClick={ack}
            disabled={busy}
            className="rounded-sm border-2 border-accent bg-accent px-2.5 py-1.5 font-mono text-[10px] font-bold uppercase tracking-[0.06em] text-on-accent hover:bg-accent-hover disabled:opacity-50"
          >
            {busy ? "…" : "Acknowledge"}
          </button>
          <button
            onClick={() => setFormOpen(false)}
            className="rounded-sm border-2 border-line px-2.5 py-1.5 font-mono text-[10px] font-semibold uppercase tracking-[0.06em] text-fg-muted hover:text-fg"
          >
            Cancel
          </button>
        </div>
      )}
    </div>
  );
}

function formatTimeAgo(dateStr: string): string {
  const date = new Date(dateStr);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffH = Math.floor(diffMin / 60);
  if (diffH < 24) return `${diffH}h ago`;
  const diffD = Math.floor(diffH / 24);
  return `${diffD}d ago`;
}
