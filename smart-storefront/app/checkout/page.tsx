import { Metadata } from "next";
import { CheckoutClient } from "../../components/storefront/checkout-client";

export const metadata: Metadata = {
  title: "Checkout",
  description: "Secure storefront checkout.",
  robots: {
    index: false,
    follow: false
  }
};

export default function CheckoutPage() {
  return (
    <div className="page-shell">
      <div className="page-heading">
        <p className="eyebrow">Secure checkout</p>
        <h1>Checkout</h1>
        <p>Enter your delivery details and review the order total.</p>
      </div>
      <CheckoutClient />
    </div>
  );
}
