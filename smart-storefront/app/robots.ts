import type { MetadataRoute } from "next";
import { getSiteUrl } from "../lib/url";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: {
      userAgent: "*",
      allow: "/",
      disallow: ["/checkout", "/account"]
    },
    sitemap: `${getSiteUrl()}/sitemap.xml`
  };
}
