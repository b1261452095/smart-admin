import { HomeCmsRenderer } from "../components/cms-renderer";
import { JsonLd } from "../components/json-ld";
import { getCmsBlocks, getProducts } from "../lib/api";
import { buildMetadata, itemListJsonLd } from "../lib/seo";
import { getSiteUrl } from "../lib/url";

export const revalidate = 300;

export const metadata = buildMetadata({
  title: "Home",
  description: "Browse featured products and curated collections from Smart Storefront.",
  path: "/"
});

export default async function HomePage() {
  const [blocks, products] = await Promise.all([getCmsBlocks(), getProducts({ limit: 8 })]);

  return (
    <>
      <JsonLd
        data={itemListJsonLd(
          products.map((product) => ({
            name: product.productName,
            url: `${getSiteUrl()}/products/${product.slug}`
          }))
        )}
      />
      <HomeCmsRenderer blocks={blocks} products={products} />
    </>
  );
}
