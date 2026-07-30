import { CartClient } from "../../components/storefront/cart-client";
import { buildMetadata } from "../../lib/seo";

export const metadata = buildMetadata({
  title: "Cart",
  description: "Review storefront cart items before checkout.",
  path: "/cart",
  noIndex: true
});

export default function CartPage() {
  return (
    <div className="page-shell">
      <div className="page-heading">
        <p className="eyebrow">Cart</p>
        <h1>Your cart</h1>
        <p>Review your selection before continuing to checkout.</p>
      </div>
      <CartClient />
    </div>
  );
}
