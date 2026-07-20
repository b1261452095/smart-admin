import Link from "next/link";
import { formatMoney } from "../lib/format";
import { Product } from "../lib/types";

export function ProductCard({ product }: { product: Product }) {
  return (
    <article className="product-card">
      <Link className="product-image-link" href={`/products/${product.slug}`} aria-label={product.productName}>
        {product.mainImage ? <img src={product.mainImage} alt={product.productName} /> : <span className="image-placeholder" />}
      </Link>
      <div className="product-card-body">
        <p className="eyebrow">{product.categoryName || "Featured"}</p>
        <h3>
          <Link href={`/products/${product.slug}`}>{product.productName}</Link>
        </h3>
        {product.subTitle ? <p>{product.subTitle}</p> : null}
        <strong>{formatMoney(product.salePriceCent, product.currency)}</strong>
      </div>
    </article>
  );
}
