"use client";

import Link from "next/link";
import { FolderPlus, List, Workflow } from "lucide-react";
import { Card } from "@/components/card";
import { Eyebrow } from "@/components/ui";

const CARDS = [
  {
    href: "/admin/projects/new",
    icon: FolderPlus,
    title: "New Project",
    description: "Create a project, link a media plan, and assign a Slack channel.",
  },
  {
    href: "/admin/projects",
    icon: List,
    title: "Manage Projects",
    description: "View, edit, and re-sync all projects and their media plans.",
  },
  {
    href: "/admin/pipeline",
    icon: Workflow,
    title: "Pipeline Control",
    description: "Trigger the daily job, run backfills, and check data freshness.",
  },
];

export default function AdminDashboard() {
  return (
    <div className="mx-auto max-w-[1100px]">
      <Eyebrow>Internal · Point Blank team only</Eyebrow>
      <h1 className="display mt-2.5 text-[38px] text-fg sm:text-[44px]">Admin</h1>
      <p className="mt-3 text-sm text-fg-muted">
        Project onboarding, media plan syncing, and pipeline management.
      </p>

      <div className="mt-7 grid gap-3.5 sm:grid-cols-2 lg:grid-cols-3">
        {CARDS.map(({ href, icon: Icon, title, description }) => (
          <Link key={href} href={href}>
            <Card className="group h-full cursor-pointer transition-all duration-base ease-snap hover:-translate-y-0.5 hover:border-line-strong">
              <div className="flex h-[38px] w-[38px] items-center justify-center rounded-sm border-[1.5px] border-tint-accent bg-tint-accent">
                <Icon className="h-[18px] w-[18px] text-accent-ink" />
              </div>
              <h2 className="mt-3.5 text-base font-bold text-fg transition-colors group-hover:text-accent-ink">
                {title}
              </h2>
              <p className="mt-1.5 text-[12.5px] leading-relaxed text-fg-muted">
                {description}
              </p>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
