/**
 * Admin layout — shared padding for the admin pages (they historically
 * rendered without a padded wrapper of their own).
 */
export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return <div className="p-5 pb-20 pt-7 sm:p-7 sm:pb-20">{children}</div>;
}
