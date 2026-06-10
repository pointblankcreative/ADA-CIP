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
  const [severity, setSeverity] = useState<string>("");

  const load = async () => {
    setLoading(true);
    try {
      const data = await api.alerts.list({
        severity: severity || undefined,
        limit: 100,
      });
      setAlerts(data);
    } catch {
      setAlerts([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [severity]);

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

  return (
    <div className="mx-auto max-w-[980px] px-5 pb-20 pt-7 sm:px-7">
      <div className="flex flex-wrap items-end justify-between gap-5">
        <div>
          <Eyebrow>Pacing · health · data integrity</Eyebrow>
          <h1 className="display mt-2.5 text-[38px] text-fg sm:text-[44px]">
            Alerts
          </h1>
          <p className="mt-3 text-sm text-fg-muted">
            {criticalCount > 0
              ? `${criticalCount} critical, ${warningCount} warning — newest first.`
              : "All clear. Signals land here the moment something drifts."}
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
        ) : alerts.length === 0 ? (
          <Card className="flex flex-col items-center py-12">
            <CheckCircle2 className="h-10 w-10 text-ok opacity-60" />
            <p className="mt-3 text-fg-muted">No alerts</p>
          </Card>
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
