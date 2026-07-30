import Link from "next/link";
import { JsonLd } from "../../../components/json-ld";
import { ProductCard } from "../../../components/product-card";
import { AddToCartButton } from "../../../components/storefront/add-to-cart-button";
import { Money } from "../../../components/storefront/money";
import { getProductBySlug, getProducts } from "../../../lib/api";
import { buildMetadata, productJsonLd } from "../../../lib/seo";

export const revalidate = 300;

type ProductPageProps = {
  params: {
    slug: string;
  };
};

export async function generateMetadata({ params }: ProductPageProps) {
  const product = await getProductBySlug(params.slug);

  if (!product) {
    return buildMetadata({
      title: "Product not found",
      description: "The product you requested could not be found.",
      path: `/products/${params.slug}`
    });
  }

  return buildMetadata({
    title: product.seoTitle || product.productName,
    description: product.seoDescription || product.subTitle || product.productName,
    path: `/products/${params.slug}`,
    image: product.mainImage
  });
}

export default async function ProductPage({ params }: ProductPageProps) {
  const product = await getProductBySlug(params.slug);

  if (!product) {
    return (
      <div className="page-shell">
        <div className="page-heading">
          <p className="eyebrow">404</p>
          <h1>Product not found</h1>
          <p>The product may have moved, or it has not been published to the storefront yet.</p>
        </div>
        <Link className="primary-link" href="/">
          Back to home
        </Link>
      </div>
    );
  }

  const related = (await getProducts({ limit: 5 })).filter((item) => item.productId !== product.productId).slice(0, 4);

  return (
    <div className="page-shell">
      <JsonLd data={productJsonLd(product)} />
      <article className="product-detail">
        <div className="product-media">
          {product.mainImage ? <img src={product.mainImage} alt={product.productName} /> : <span className="image-placeholder" />}
        </div>
        <div className="product-summary">
          <p className="eyebrow">{product.categoryName || "Product"}</p>
          <h1>{product.productName}</h1>
          {product.subTitle ? <p className="detail-copy">{product.subTitle}</p> : null}
          <div className="price">
            <Money valueCent={product.salePriceCent} currency={product.currency} />
          </div>
          <AddToCartButton product={product} />
          <p className="detail-copy">{product.productDetail || product.subTitle || "Explore this piece from the current collection."}</p>
        </div>
      </article>

      {related.length ? (
        <section className="section-band">
          <div className="section-heading">
            <p className="eyebrow">More to explore</p>
            <h2>Related products</h2>
          </div>
          <div className="product-grid">
            {related.map((item) => (
              <ProductCard key={item.productId} product={item} />
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
