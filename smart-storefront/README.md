# Smart Storefront

`smart-storefront` is the SEO-friendly customer storefront for the SmartAdmin shop module.

## Stack

- Next.js App Router
- Server components by default
- SSR/ISR for home, collection, product, search, sitemap, and robots
- SmartAdmin `ResponseDTO` API adapter

## Local Run

```bash
npm install
npm run dev
```

The dev server uses port `3100`.

## Environment

Copy `.env.example` to `.env.local` when you need local overrides.

```bash
NEXT_PUBLIC_SITE_URL=http://localhost:3100
NEXT_PUBLIC_STOREFRONT_API_BASE_URL=http://localhost:1024
NEXT_PUBLIC_STORE_TENANT_ID=1
```

## API Notes

The current backend already exposes CMS blocks at:

```text
POST /shop/client/cms/block/list
```

This storefront also prepares adapters for the recommended public product/category endpoints:

```text
POST /shop/client/product/queryPage
GET  /shop/client/product/getBySlug/{slug}
POST /shop/client/category/tree
```

Until those backend endpoints are added, the pages fall back to demo data so the SEO page structure can be developed first.
