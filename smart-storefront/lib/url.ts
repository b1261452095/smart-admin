export function getSiteUrl() {
  return trimTrailingSlash(process.env.NEXT_PUBLIC_SITE_URL || "http://localhost:3100");
}

export function getApiBaseUrl() {
  return trimTrailingSlash(process.env.NEXT_PUBLIC_STOREFRONT_API_BASE_URL || "http://localhost:1024");
}

export function getTenantId() {
  return Number(process.env.NEXT_PUBLIC_STORE_TENANT_ID || "1");
}

export function getStoreName() {
  return process.env.NEXT_PUBLIC_STORE_NAME || "Smart Storefront";
}

export function trimTrailingSlash(value: string) {
  return value.replace(/\/+$/, "");
}

export function slugify(value: string) {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function makeProductSlug(productName: string, productId: number, productCode?: string) {
  const base = slugify(productName || productCode || String(productId));
  return `${base || "product"}-${productId}`;
}

export function makeCategorySlug(categoryName: string, categoryId: number, categoryCode?: string) {
  return slugify(categoryCode || categoryName || String(categoryId)) || `category-${categoryId}`;
}

export function extractIdFromSlug(slug: string) {
  const match = slug.match(/-(\d+)$/);
  return match ? Number(match[1]) : undefined;
}
