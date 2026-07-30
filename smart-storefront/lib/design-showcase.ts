import { mockProducts } from "./mock-data";
import { Product } from "./types";

export function fillShowcaseProducts(products: Product[], limit = 8) {
  const unique = new Map<number, Product>();

  [...products, ...mockProducts].forEach((product) => {
    if (!unique.has(product.productId)) {
      unique.set(product.productId, product);
    }
  });

  return Array.from(unique.values()).slice(0, limit);
}
