import Link from "next/link";
import { Metadata } from "next";

export const metadata: Metadata = {
  title: "Order received",
  robots: {
    index: false,
    follow: false
  }
};

export default function CheckoutSuccessPage({ searchParams }: { searchParams?: { order?: string } }) {
  const orderNo = searchParams?.order || "demo-order";

  return (
    <div className="page-shell">
      <div className="empty-state success-state">
        <p className="eyebrow">Order received</p>
        <h1>Thank you</h1>
        <p>Your order reference is {orderNo}. Keep it for your records.</p>
        <div className="action-row">
          <Link className="primary-link" href="/search">
            Continue shopping
          </Link>
          <Link className="secondary-link" href="/account/orders">
            View orders
          </Link>
        </div>
      </div>
    </div>
  );
}
