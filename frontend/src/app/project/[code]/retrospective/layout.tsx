/**
 * MIGRATION PIN — the retrospective screen is still slate-styled
 * (re-skin Phase 9). Pinning it dark keeps the legacy palette coherent
 * on the light app. Delete this layout when retro moves to tokens.
 */
export default function RetrospectiveLayout({
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
