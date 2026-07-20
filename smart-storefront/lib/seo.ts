import type { Metadata } from "next";
import { Product } from "./types";
import { formatMoney } from "./format";
import { getSiteUrl, getStoreName } from "./url";

type SeoInput = {
  title: string;
  description: string;
  path?: string;
  image?: string;
};

export function buildMetadata(input: SeoInput): Metadata {
  const siteUrl = getSiteUrl();
  const url = `${siteUrl}${input.path || ""}`;
  const title = input.title.includes(getStoreName()) ? input.title : `${input.title} | ${getStoreName()}`;

  return {
    metadataBase: new URL(siteUrl),
    title,
    description: input.description,
    alternates: {
      canonical: url
    },
    openGraph: {
      title,
      description: input.description,
      url,
      siteName: getStoreName(),
      images: input.image ? [{ url: input.image }] : undefined,
      type: "website"
    },
    twitter: {
      card: "summary_large_image",
      title,
      description: input.description,
      images: input.image ? [input.image] : undefined
    }
  };
}

export function organizationJsonLd() {
  return {
    "@context": "https://schema.org",
    "@type": "Organization",
    name: getStoreName(),
    url: getSiteUrl()
  };
}

export function websiteJsonLd() {
  return {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: getStoreName(),
    url: getSiteUrl(),
    potentialAction: {
      "@type": "SearchAction",
      target: `${getSiteUrl()}/search?q={search_term_string}`,
      "query-input": "required name=search_term_string"
    }
  };
}

export function productJsonLd(product: Product) {
  return {
    "@context": "https://schema.org",
    "@type": "Product",
    name: product.productName,
    description: product.seoDescription || product.subTitle,
    image: product.mainImage ? [product.mainImage] : undefined,
    sku: product.productCode,
    offers: {
      "@type": "Offer",
      priceCurrency: product.currency,
      price: formatMoney(product.salePriceCent, product.currency).replace(/[^0-9.]/g, ""),
      availability: product.shelvesFlag === false ? "https://schema.org/OutOfStock" : "https://schema.org/InStock",
      url: `${getSiteUrl()}/products/${product.slug}`
    }
  };
}

export function itemListJsonLd(items: Array<{ name: string; url: string }>) {
  return {
    "@context": "https://schema.org",
    "@type": "ItemList",
    itemListElement: items.map((item, index) => ({
      "@type": "ListItem",
      position: index + 1,
      name: item.name,
      url: item.url
    }))
  };
}
