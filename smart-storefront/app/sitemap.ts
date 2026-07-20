import type { MetadataRoute } from "next";
import { getCategories, getProducts } from "../lib/api";
import { getSiteUrl } from "../lib/url";

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const siteUrl = getSiteUrl();
  const [categories, products] = await Promise.all([getCategories(), getProducts({ limit: 100 })]);

  return [
    {
      url: siteUrl,
      lastModified: new Date(),
      changeFrequency: "daily",
      priority: 1
    },
    {
      url: `${siteUrl}/search`,
      lastModified: new Date(),
      changeFrequency: "weekly",
      priority: 0.4
    },
    ...categories.map((category) => ({
      url: `${siteUrl}/collections/${category.slug}`,
      lastModified: new Date(),
      changeFrequency: "daily" as const,
      priority: 0.8
    })),
    ...products.map((product) => ({
      url: `${siteUrl}/products/${product.slug}`,
      lastModified: new Date(),
      changeFrequency: "daily" as const,
      priority: 0.9
    }))
  ];
}
