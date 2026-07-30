import Link from "next/link";
import { AccountShell } from "../../components/storefront/account-shell";
import { buildMetadata } from "../../lib/seo";

export const metadata = buildMetadata({
  title: "Account",
  description: "Storefront account overview for orders, addresses, and settings.",
  path: "/account",
  noIndex: true
});

export default function AccountPage() {
  return (
    <div className="page-shell">
      <AccountShell>
        <div className="page-heading compact">
          <p className="eyebrow">Overview</p>
          <h1>Account center</h1>
          <p>Manage orders, delivery addresses, and account preferences.</p>
        </div>
        <div className="account-cards">
          <Link href="/account/orders">My orders</Link>
          <Link href="/account/addresses">Address book</Link>
          <Link href="/account/settings">Security and preferences</Link>
        </div>
      </AccountShell>
    </div>
  );
}
