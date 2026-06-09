/**
 * MIGRATION PIN — the project detail + retrospective screens are still
 * slate-styled (re-skin Phases 4–7 and 9). Pinning them dark keeps the
 * legacy palette coherent on the light app. When the project shell is
 * rebuilt (Phase 4), this pin moves down to retrospective/ only.
 */
export default function ProjectLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div data-theme="dark" className="min-h-[calc(100vh-58px)]">
      {children}
    </div>
  );
}
