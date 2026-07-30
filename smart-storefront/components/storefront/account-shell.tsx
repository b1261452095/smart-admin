import Link from "next/link";
import { ReactNode } from "react";

const navItems = [
  { href: "/account", label: "Overview" },
  { href: "/account/orders", label: "Orders" },
  { href: "/account/addresses", label: "Addresses" },
  { href: "/account/settings", label: "Settings" }
];

export function AccountShell({ children }: { children: ReactNode }) {
  return (
    <div className="account-layout">
      <aside className="account-sidebar">
        <p className="eyebrow">Account</p>
        <h2>My account</h2>
        <nav>
          {navItems.map((item) => (
            <Link key={item.href} href={item.href}>
              {item.label}
            </Link>
          ))}
        </nav>
      </aside>
      <section className="account-content">{children}</section>
    </div>
  );
}
