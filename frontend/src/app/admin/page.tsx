"use client";

import Link from "next/link";
import { FolderPlus, List, Workflow, Plus } from "lucide-react";
import { Card } from "@/components/card";

const CARDS = [
  {
    href: "/admin/projects/new",
    icon: FolderPlus,
    title: "New Project",
    description: "Create a project, link a media plan, and assign a Slack channel.",
    accent: "text-brand-400",
  },
  {
    href: "/admin/projects",
    icon: List,
    title: "Manage Projects",
    description: "View, edit, and re-sync all projects and their media plans.",
    accent: "text-emerald-400",
  },
  {
    href: "/admin/pipeline",
    icon: Workflow,
    title: "Pipeline Control",
    description: "Trigger the daily job, run backfills, and check data freshness.",
    accent: "text-amber-400",
  },
];

export default function AdminDashboard() {
  return (
    <div>
      <h1 className="text-xl font-semibold text-white">Admin</h1>
      <p className="mt-1 text-sm text-slate-400">
        Project onboarding, media plan syncing, and pipeline management.
      </p>

      <div className="mt-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {CARDS.map(({ href, icon: Icon, title, description, accent }) => (
          <Link key={href} href={href}>
            <Card className="group cursor-pointer transition-colors hover:border-slate-700">
              <div className="flex items-start gap-3">
                <div className={`mt-0.5 rounded-md bg-slate-800 p-2 ${accent}`}>
                  <Icon className="h-5 w-5" />
                </div>
                <div>
                  <h2 className="font-medium text-white group-hover:text-brand-400 transition-colors">
                    {title}
                  </h2>
                  <p className="mt-1 text-xs text-slate-400 leading-relaxed">
                    {description}
                  </p>
                </div>
              </div>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
