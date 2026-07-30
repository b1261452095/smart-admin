import type { Metadata } from "next";
import "@fontsource/bodoni-moda/400.css";
import "@fontsource/manrope/400.css";
import "@fontsource/manrope/700.css";
import "../tokens.css";
import "./globals.css";
import "./storefront.css";
import { SiteFooter } from "../components/site-footer";
import { SiteHeader } from "../components/site-header";
import { StorefrontMotionProvider } from "../components/storefront/motion-provider";
import { getCategories, getCmsBlocks } from "../lib/api";
import { organizationJsonLd, websiteJsonLd } from "../lib/seo";
import { getSiteUrl, getStoreName } from "../lib/url";
import { JsonLd } from "../components/json-ld";

export const metadata: Metadata = {
  metadataBase: new URL(getSiteUrl()),
  title: {
    default: getStoreName(),
    template: `%s | ${getStoreName()}`
  },
  description: "Discover considered intimates, sleepwear, and jewelry for everyday self-expression."
};

export default async function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const [categories, announcementBlocks] = await Promise.all([getCategories(), getCmsBlocks(7)]);
  const announcement = announcementBlocks[0];

  return (
    <html lang="en">
      <body>
        <StorefrontMotionProvider>
          <JsonLd data={organizationJsonLd()} />
          <JsonLd data={websiteJsonLd()} />
          <SiteHeader
            categories={categories}
            announcement={announcement?.blockTitle}
            announcementHref={announcement?.linkUrl}
          />
          <main className="site-main">{children}</main>
          <SiteFooter />
        </StorefrontMotionProvider>
      </body>
    </html>
  );
}
