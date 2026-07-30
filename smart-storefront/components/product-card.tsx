import Link from "next/link";
import { Product } from "../lib/types";
import { Money } from "./storefront/money";

export function ProductCard({ product }: { product: Product }) {
  return (
    <article className="product-card">
      <Link className="product-image-link" href={`/products/${product.slug}`} aria-label={product.productName}>
        {product.mainImage ? <img src={product.mainImage} alt={product.productName} loading="lazy" /> : <span className="image-placeholder" />}
      </Link>
      <div className="product-card-body">
        <p className="eyebrow">{product.categoryName || "Featured"}</p>
        <h3>
          <Link href={`/products/${product.slug}`}>{product.productName}</Link>
        </h3>
        <strong className="product-card-price">
          <Money valueCent={product.salePriceCent} currency={product.currency} />
        </strong>
      </div>
    </article>
  );
}
