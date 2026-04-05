import type { Metadata } from "next";
import { Sidebar } from "@/components/sidebar";
import "./globals.css";

export const metadata: Metadata = {
  title: "CIP — Campaign Intelligence Platform",
  description:
    "Campaign monitoring, budget pacing, and automated reporting for Point Blank Creative",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen font-sans">
        <Sidebar />
        <main className="min-h-screen md:ml-56">{children}</main>
      </body>
    </html>
  );
}
