"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertTriangle,
  CheckCircle2,
  Clock,
  Info,
  Filter,
  RefreshCw,
} from "lucide-react";
import { api, type Alert } from "@/lib/api";
import { Card } from "@/components/card";
import { cn, severityColor } from "@/lib/utils";

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
  }, [severity]);

  const criticalCount = alerts.filter((a) => a.severity === "critical").length;
  const warningCount = alerts.filter((a) => a.severity === "warning").length;

  return (
    <div className="p-6 lg:p-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white">
            Alerts
          </h1>
          <p className="mt-1 text-sm text-slate-500">
            {criticalCount > 0
              ? `${criticalCount} critical, ${warningCount} warning`
              : "All clear"}
          </p>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-2 rounded-md border border-slate-700 bg-surface-raised px-3.5 py-2 text-sm text-slate-300 transition-colors hover:bg-slate-700 disabled:opacity-50"
        >
          <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="mt-5 flex items-center gap-2">
        <Filter className="h-4 w-4 text-slate-500" />
        {["", "critical", "warning", "info"].map((sev) => (
          <button
            key={sev}
            onClick={() => setSeverity(sev)}
            className={cn(
              "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
              severity === sev
                ? "bg-brand-600/20 text-brand-400"
                : "text-slate-500 hover:bg-slate-800 hover:text-slate-300"
            )}
          >
            {sev || "All"}
          </button>
        ))}
      </div>

      {/* Alert list */}
      <div className="mt-5 space-y-2">
        {loading ? (
          Array.from({ length: 5 }).map((_, i) => (
            <Card key={i} className="animate-pulse">
              <div className="h-4 w-48 rounded bg-slate-700" />
              <div className="mt-2 h-3 w-72 rounded bg-slate-700" />
            </Card>
          ))
        ) : alerts.length === 0 ? (
          <Card className="flex flex-col items-center py-12">
            <CheckCircle2 className="h-10 w-10 text-emerald-500/50" />
            <p className="mt-3 text-slate-400">No alerts</p>
          </Card>
        ) : (
          alerts.map((alert) => (
            <AlertRow key={alert.alert_id} alert={alert} />
          ))
        )}
      </div>
    </div>
  );
}

function AlertRow({ alert }: { alert: Alert }) {
  const SeverityIcon =
    alert.severity === "critical"
      ? AlertTriangle
      : alert.severity === "warning"
      ? AlertTriangle
      : Info;

  const timeAgo = formatTimeAgo(alert.created_at);

  return (
    <Card className="!p-4">
      <div className="flex items-start gap-3">
        <div
          className={cn(
            "flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md border",
            severityColor(alert.severity)
          )}
        >
          <SeverityIcon className="h-4 w-4" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-white">
              {alert.title}
            </span>
            <span
              className={cn(
                "inline-flex rounded-full border px-2 py-0.5 text-[10px] font-medium",
                severityColor(alert.severity)
              )}
            >
              {alert.severity}
            </span>
            <Link
              href={`/project/${alert.project_code}`}
              className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[10px] text-slate-400 hover:text-brand-400 transition-colors"
            >
              {alert.project_code}
            </Link>
          </div>
          <p className="mt-0.5 text-xs text-slate-400">{alert.message}</p>
          <div className="mt-2 flex items-center gap-3 text-[10px] text-slate-500">
            <span className="flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {timeAgo}
            </span>
            <span className="rounded bg-slate-800 px-1.5 py-0.5">
              {alert.alert_type}
            </span>
            {alert.acknowledged_at && (
              <span className="flex items-center gap-1 text-emerald-500">
                <CheckCircle2 className="h-3 w-3" />
                Acknowledged
              </span>
            )}
          </div>
        </div>
      </div>
    </Card>
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
