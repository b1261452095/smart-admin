import { AccountShell } from "../../../components/storefront/account-shell";
import { formatMoney } from "../../../lib/format";
import { storefrontApi } from "../../../lib/storefront-api";
import { buildMetadata } from "../../../lib/seo";

export const metadata = buildMetadata({
  title: "Orders",
  description: "View storefront order history and tracking status.",
  path: "/account/orders",
  noIndex: true
});

export default async function AccountOrdersPage() {
  const orders = await storefrontApi.orders();

  return (
    <div className="page-shell">
      <AccountShell>
        <div className="page-heading compact">
          <p className="eyebrow">Orders</p>
          <h1>My orders</h1>
          <p>Review recent purchases and their current status.</p>
        </div>
        <div className="order-list">
          {orders.map((order) => (
            <article className="order-card" key={order.orderId}>
              <div>
                <h3>{order.orderNo}</h3>
                <p>
                  {order.status} · {order.itemCount} items · {order.createTime}
                </p>
              </div>
              <strong>{formatMoney(order.amountCent, order.currency)}</strong>
            </article>
          ))}
        </div>
      </AccountShell>
    </div>
  );
}
