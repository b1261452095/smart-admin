import Link from "next/link";
import { getStoreName } from "../lib/url";
import { RegionSwitcher } from "./storefront/region-switcher";

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="footer-mast">
        <Link className="footer-wordmark" href="/">
          {getStoreName()}
        </Link>
        <p>Intimates, sleepwear, and jewelry selected for your own rhythm.</p>
      </div>
      <div className="footer-directory">
        <nav className="footer-links" aria-label="Footer navigation">
          <Link href="/search">Shop all</Link>
          <Link href="/cart">Cart</Link>
          <Link href="/account">Account</Link>
        </nav>
        <RegionSwitcher />
      </div>
    </footer>
  );
}
