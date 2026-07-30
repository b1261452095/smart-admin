import { HomeCmsRenderer } from "../components/cms-renderer";
import { JsonLd } from "../components/json-ld";
import { getCmsBlocks, getProducts } from "../lib/api";
import { buildMetadata, itemListJsonLd } from "../lib/seo";
import { getSiteUrl } from "../lib/url";

export const revalidate = 300;

export const metadata = buildMetadata({
  title: "Home",
  description: "Discover considered intimates, sleepwear, and jewelry for everyday self-expression.",
  path: "/"
});

export default async function HomePage() {
  const [blocks, products] = await Promise.all([getCmsBlocks(), getProducts({ limit: 16 })]);

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
