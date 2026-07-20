import { JsonLd } from "../../../components/json-ld";
import { ProductCard } from "../../../components/product-card";
import { getCategories, getProducts } from "../../../lib/api";
import { buildMetadata, itemListJsonLd } from "../../../lib/seo";
import { getSiteUrl } from "../../../lib/url";

export const revalidate = 300;

type CollectionPageProps = {
  params: {
    slug: string;
  };
};

export async function generateMetadata({ params }: CollectionPageProps) {
  const categories = await getCategories();
  const category = categories.find((item) => item.slug === params.slug);
  const title = category?.categoryName || "Collection";

  return buildMetadata({
    title,
    description: `Shop ${title} products with fast server-rendered pages and clean SEO metadata.`,
    path: `/collections/${params.slug}`,
    image: category?.categoryImage
  });
}

export default async function CollectionPage({ params }: CollectionPageProps) {
  const categories = await getCategories();
  const category = categories.find((item) => item.slug === params.slug);
  const products = await getProducts({ categorySlug: params.slug, limit: 24 });
  const title = category?.categoryName || "Collection";

  return (
    <div className="page-shell">
      <JsonLd
        data={itemListJsonLd(
          products.map((product) => ({
            name: product.productName,
            url: `${getSiteUrl()}/products/${product.slug}`
          }))
        )}
      />
      <div className="page-heading">
        <p className="eyebrow">Collection</p>
        <h1>{title}</h1>
        <p>Server-rendered collection pages give search engines stable URLs, readable content, and product links.</p>
      </div>
      <div className="product-grid">
        {products.map((product) => (
          <ProductCard key={product.productId} product={product} />
        ))}
      </div>
    </div>
  );
}
