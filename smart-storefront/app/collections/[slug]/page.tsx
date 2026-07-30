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
    description: `Explore the latest ${title} pieces in the collection.`,
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
        <p>Explore the current selection and find a piece that feels like your own.</p>
      </div>
      {products.length ? (
        <div className="product-grid">
          {products.map((product) => (
            <ProductCard key={product.productId} product={product} />
          ))}
        </div>
      ) : (
        <div className="empty-state">
          <h2>No pieces found</h2>
          <p>This collection is being updated. Explore the full catalogue in the meantime.</p>
        </div>
      )}
    </div>
  );
}
