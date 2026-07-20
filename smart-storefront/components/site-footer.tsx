import { getStoreName } from "../lib/url";

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div>
        <strong>{getStoreName()}</strong>
        <p>Independent storefront powered by SmartAdmin shop APIs.</p>
      </div>
      <div className="footer-links">
        <a href="/shipping-policy">Shipping</a>
        <a href="/refund-policy">Refunds</a>
        <a href="/privacy-policy">Privacy</a>
      </div>
    </footer>
  );
}
