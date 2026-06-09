/**
 * MIGRATION PIN — the alerts page is still slate-styled (re-skin Phase 8).
 * Pinning it dark keeps the legacy palette coherent on the light app.
 * Delete this layout when the alerts screen moves to tokens.
 */
export default function AlertsLayout({
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
