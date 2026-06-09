/**
 * MIGRATION PIN — admin pages are still slate-styled (re-skin Phase 9).
 * Pinning them dark keeps the legacy palette coherent on the light app.
 * Delete this layout when the admin screens move to tokens.
 */
export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div data-theme="dark" className="min-h-[calc(100vh-58px)] p-6 lg:p-8">
      {children}
    </div>
  );
}
