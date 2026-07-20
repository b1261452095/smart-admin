import type { Metadata } from "next";
import "./globals.css";
import { SiteFooter } from "../components/site-footer";
import { SiteHeader } from "../components/site-header";
import { getCategories } from "../lib/api";
import { organizationJsonLd, websiteJsonLd } from "../lib/seo";
import { getSiteUrl, getStoreName } from "../lib/url";
import { JsonLd } from "../components/json-ld";

export const metadata: Metadata = {
  metadataBase: new URL(getSiteUrl()),
  title: {
    default: getStoreName(),
    template: `%s | ${getStoreName()}`
  },
  description: "A server-rendered ecommerce storefront powered by SmartAdmin shop APIs."
};

export default async function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const categories = await getCategories();

  return (
    <html lang="en">
      <body>
        <JsonLd data={organizationJsonLd()} />
        <JsonLd data={websiteJsonLd()} />
        <SiteHeader categories={categories} />
        <main className="site-main">{children}</main>
        <SiteFooter />
      </body>
    </html>
  );
}
