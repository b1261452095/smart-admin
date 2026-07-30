import Link from "next/link";
import { Money } from "../../../components/storefront/money";
import { getProducts } from "../../../lib/api";
import { fillShowcaseProducts } from "../../../lib/design-showcase";
import { buildMetadata } from "../../../lib/seo";
import styles from "../design.module.css";

export const metadata = buildMetadata({
  title: "High Conversion Design Preview",
  description: "A conversion-focused ecommerce storefront design preview.",
  path: "/design/conversion",
  noIndex: true
});

export default async function ConversionDesignPage() {
  const products = fillShowcaseProducts(await getProducts({ limit: 8 }), 8);

  return (
    <div className={`design-preview-root ${styles.previewRoot} ${styles.conversionPage}`}>
      <nav className={styles.designSwitcher} aria-label="Design preview">
        <Link href="/design/boutique">Premium boutique</Link>
        <Link className={styles.activeSwitch} href="/design/conversion">
          High conversion
        </Link>
      </nav>

      <div className={styles.conversionAnnouncement}>Summer event: save 20% on two or more pieces with code SHINE20</div>

      <header className={styles.conversionHeader}>
        <div className={styles.conversionHeaderMain}>
          <details className={styles.mobileMenu}>
            <summary>Menu</summary>
            <nav>
              <Link href="/collections/bracelets">Bracelets</Link>
              <Link href="/collections/necklaces">Necklaces</Link>
              <Link href="/collections/rings">Rings</Link>
              <Link href="/search">Gifts under $50</Link>
            </nav>
          </details>
          <Link className={styles.conversionLogo} href="/design/conversion">
            LUMINA
          </Link>
          <form className={styles.conversionSearch} action="/search">
            <input name="q" aria-label="Search products" placeholder="Search jewelry, gifts and collections" />
            <button type="submit">Search</button>
          </form>
          <nav className={styles.conversionUtilities} aria-label="Customer links">
            <Link href="/account">My account</Link>
            <Link href="/cart">Cart (0)</Link>
          </nav>
        </div>
        <nav className={styles.conversionCategoryNav} aria-label="Product categories">
          <Link href="/collections/bracelets">Bracelets</Link>
          <Link href="/collections/necklaces">Necklaces</Link>
          <Link href="/collections/rings">Rings</Link>
          <Link href="/collections/earrings">Earrings</Link>
          <Link href="/search">New arrivals</Link>
          <Link href="/search">Gifts under $50</Link>
        </nav>
      </header>

      <section className={styles.conversionHero}>
        <div className={styles.conversionHeroImage}>
          <img src={products[0]?.mainImage} alt="" />
        </div>
        <div className={styles.conversionHeroContent}>
          <p className={styles.conversionKicker}>Limited-time summer event</p>
          <h1>Everyday shine, better together.</h1>
          <p>Build your perfect stack and save 20% when you choose any two pieces. Easy gifting, effortless returns.</p>
          <div className={styles.conversionActions}>
            <Link className={styles.conversionPrimary} href="/search">
              Shop best sellers
            </Link>
            <Link className={styles.conversionSecondary} href="/collections/bracelets">
              Shop by category
            </Link>
          </div>
          <p className={styles.conversionHeroNote}>Offer ends Sunday. Discount applied automatically at checkout.</p>
        </div>
      </section>

      <section className={styles.conversionBenefits} aria-label="Shopping benefits">
        <div className={styles.conversionBenefit}>
          <strong>Free delivery over $75</strong>
          <span>Fast, tracked shipping</span>
        </div>
        <div className={styles.conversionBenefit}>
          <strong>30-day returns</strong>
          <span>Simple and stress-free</span>
        </div>
        <div className={styles.conversionBenefit}>
          <strong>2-year warranty</strong>
          <span>Coverage on every piece</span>
        </div>
        <div className={styles.conversionBenefit}>
          <strong>4.9 / 5 from 2,000+ reviews</strong>
          <span>Loved by everyday collectors</span>
        </div>
      </section>

      <section className={styles.conversionCategories}>
        <div className={styles.conversionSectionHeading}>
          <div>
            <h2>Shop by category</h2>
            <p>Find the piece that fits your style.</p>
          </div>
          <Link href="/search">View all categories</Link>
        </div>
        <div className={styles.conversionCategoryGrid}>
          {products.slice(0, 4).map((product) => (
            <Link
              className={styles.conversionCategoryCard}
              href={`/collections/${product.categoryName?.toLowerCase()}`}
              key={product.productId}
            >
              <img src={product.mainImage} alt={product.categoryName || product.productName} />
              <strong>Shop {product.categoryName || "the collection"}</strong>
            </Link>
          ))}
        </div>
      </section>

      <section className={styles.conversionProducts}>
        <div className={styles.conversionSectionHeading}>
          <div>
            <h2>Best sellers</h2>
            <p>Customer favorites, ready to ship.</p>
          </div>
          <Link href="/search">Shop all best sellers</Link>
        </div>
        <div className={styles.conversionGrid}>
          {products.slice(0, 8).map((product, index) => (
            <article className={styles.conversionProductCard} key={product.productId}>
              {index < 3 ? <span className={styles.conversionBadge}>{index === 0 ? "BEST SELLER" : "SAVE 15%"}</span> : null}
              <Link className={styles.conversionProductImage} href={`/products/${product.slug}`}>
                <img src={product.mainImage} alt={product.productName} />
              </Link>
              <div className={styles.conversionProductInfo}>
                <span className={styles.conversionRating}>Rated 4.9 / 5</span>
                <h3>
                  <Link href={`/products/${product.slug}`}>{product.productName}</Link>
                </h3>
                <p>{product.subTitle}</p>
                <div className={styles.conversionPrice}>
                  <Money valueCent={product.salePriceCent} currency={product.currency} />
                  {index < 3 ? (
                    <span className={styles.conversionCompare}>
                      <Money valueCent={Math.round(product.salePriceCent * 1.18)} currency={product.currency} />
                    </span>
                  ) : null}
                </div>
                <Link className={styles.conversionQuickAdd} href={`/products/${product.slug}`}>
                  Choose options
                </Link>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className={styles.conversionCampaign}>
        <div className={styles.conversionCampaignCopy}>
          <span>Easy gifts, beautifully handled</span>
          <h2>Need it to feel special? We have the details covered.</h2>
          <p>Gift-ready packaging, optional message cards, and a simple 30-day return window for every order.</p>
          <Link className={styles.conversionPrimary} href="/search">
            Shop gifts under $50
          </Link>
        </div>
        <div className={styles.conversionCampaignImage}>
          <img src={products[3]?.mainImage || products[1]?.mainImage} alt="Gift-ready jewelry" />
        </div>
      </section>

      <section className={styles.conversionReviews}>
        <div className={styles.conversionSectionHeading}>
          <div>
            <h2>Why customers come back</h2>
            <p>Recent verified reviews from the Lumina community.</p>
          </div>
        </div>
        <div className={styles.conversionReviewGrid}>
          <article className={styles.conversionReview}>
            <strong>5.0 / 5 - Beautiful quality</strong>
            <p>The finish is even better in person, and the packaging made it feel like a proper gift.</p>
            <span>Verified buyer - Maya R.</span>
          </article>
          <article className={styles.conversionReview}>
            <strong>5.0 / 5 - Arrived so quickly</strong>
            <p>I ordered on Monday and had it in time for the weekend. The fit guide was spot on.</p>
            <span>Verified buyer - Emma T.</span>
          </article>
          <article className={styles.conversionReview}>
            <strong>4.9 / 5 - Easy to wear</strong>
            <p>Lightweight, comfortable, and it works with everything. I already came back for another color.</p>
            <span>Verified buyer - Sofia L.</span>
          </article>
        </div>
      </section>

      <footer className={styles.previewFooter}>
        <div>
          <strong>LUMINA</strong>
          <p>Everyday jewelry, thoughtful service, and gifts people are genuinely excited to open.</p>
        </div>
        <nav className={styles.previewFooterLinks} aria-label="Footer links">
          <Link href="/search">Help center</Link>
          <Link href="/shipping-policy">Shipping</Link>
          <Link href="/refund-policy">Returns</Link>
          <Link href="/account">Order status</Link>
        </nav>
      </footer>
    </div>
  );
}
