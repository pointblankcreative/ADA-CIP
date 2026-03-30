"use client";

import { useEffect, useState } from "react";
import { AlertTriangle, CheckCircle2, Clock, Info } from "lucide-react";
import { api, type Alert } from "@/lib/api";
import { Card } from "@/components/card";
import { cn, severityColor } from "@/lib/utils";

export function AlertsTab({ code }: { code: string }) {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.alerts
      .list({ project_code: code, limit: 50 })
      .then(setAlerts)
      .catch(() => setAlerts([]))
      .finally(() => setLoading(false));
  }, [code]);

  if (loading) {
    return (
      <div className="space-y-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <Card key={i} className="animate-pulse">
            <div className="h-4 w-48 rounded bg-slate-700" />
            <div className="mt-2 h-3 w-64 rounded bg-slate-700" />
          </Card>
        ))}
      </div>
    );
  }

  if (alerts.length === 0) {
    return (
      <Card className="flex flex-col items-center py-12">
        <CheckCircle2 className="h-10 w-10 text-emerald-500/50" />
        <p className="mt-3 text-slate-400">No alerts for this project</p>
      </Card>
    );
  }

  return (
    <div className="space-y-3">
      {alerts.map((alert) => (
        <AlertCard key={alert.alert_id} alert={alert} />
      ))}
    </div>
  );
}

function AlertCard({ alert }: { alert: Alert }) {
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
