import Link from "next/link";
import { Money } from "../../../components/storefront/money";
import { getProducts } from "../../../lib/api";
import { fillShowcaseProducts } from "../../../lib/design-showcase";
import { buildMetadata } from "../../../lib/seo";
import styles from "../design.module.css";

export const metadata = buildMetadata({
  title: "Boutique Design Preview",
  description: "A premium editorial storefront design preview.",
  path: "/design/boutique",
  noIndex: true
});

export default async function BoutiqueDesignPage() {
  const products = fillShowcaseProducts(await getProducts({ limit: 8 }), 8);
  const collectionProducts = products.slice(0, 3);

  return (
    <div className={`design-preview-root ${styles.previewRoot} ${styles.boutiquePage}`}>
      <nav className={styles.designSwitcher} aria-label="Design preview">
        <Link className={styles.activeSwitch} href="/design/boutique">
          Premium boutique
        </Link>
        <Link href="/design/conversion">High conversion</Link>
      </nav>

      <div className={styles.boutiqueAnnouncement}>Complimentary delivery and returns on orders over $75</div>

      <header className={styles.boutiqueHeader}>
        <nav className={styles.boutiqueNav} aria-label="Boutique navigation">
          <Link href="/collections/bracelets">New arrivals</Link>
          <Link href="/collections/necklaces">Jewelry</Link>
          <Link href="/search">Gifts</Link>
        </nav>
        <details className={styles.mobileMenu}>
          <summary>Menu</summary>
          <nav>
            <Link href="/collections/bracelets">New arrivals</Link>
            <Link href="/collections/necklaces">Jewelry</Link>
            <Link href="/search">Gifts</Link>
            <Link href="/account">Account</Link>
          </nav>
        </details>
        <Link className={styles.boutiqueLogo} href="/design/boutique">
          AURELIA
        </Link>
        <nav className={styles.boutiqueUtility} aria-label="Customer links">
          <Link href="/search">Search</Link>
          <Link href="/account">Account</Link>
          <Link href="/cart">Bag (0)</Link>
        </nav>
        <Link className={styles.mobileMenu} href="/cart">
          Bag
        </Link>
      </header>

      <section className={styles.boutiqueHero}>
        <div className={styles.boutiqueHeroImage}>
          <img src={products[3]?.mainImage || products[0]?.mainImage} alt="" />
        </div>
        <div className={styles.boutiqueHeroContent}>
          <p className={styles.boutiqueKicker}>The pearl edit</p>
          <h1>Modern heirlooms, made to be lived in.</h1>
          <p>Natural stones, luminous pearls, and thoughtful details for the rituals of every day.</p>
          <div className={styles.boutiqueActions}>
            <Link className={styles.boutiquePrimary} href="/collections/necklaces">
              Explore the collection
            </Link>
            <Link className={styles.boutiqueSecondary} href="/search">
              Discover our story
            </Link>
          </div>
        </div>
      </section>

      <section className={styles.boutiqueIntro}>
        <p className={styles.sectionKicker}>A quiet kind of luxury</p>
        <div>
          <h2>Jewelry with presence, designed without excess.</h2>
          <p className={styles.boutiqueIntroCopy}>
            Each piece balances natural texture with clean form. Considered proportions make every design easy to wear,
            layer, and keep close.
          </p>
        </div>
      </section>

      <section className={styles.boutiqueCollections} aria-label="Curated collections">
        {collectionProducts.map((product, index) => (
          <Link className={styles.boutiqueCollectionCard} href={`/collections/${product.categoryName?.toLowerCase()}`} key={product.productId}>
            <img src={product.mainImage} alt={product.categoryName || product.productName} />
            <span className={styles.boutiqueCollectionLabel}>
              <span>Collection 0{index + 1}</span>
              <strong>{product.categoryName || product.productName}</strong>
            </span>
          </Link>
        ))}
      </section>

      <section className={styles.boutiqueProducts}>
        <div className={styles.boutiqueSectionHeading}>
          <div>
            <p className={styles.sectionKicker}>Curated for you</p>
            <h2>Everyday signatures</h2>
          </div>
          <Link href="/search">View all pieces</Link>
        </div>
        <div className={styles.boutiqueGrid}>
          {products.slice(0, 4).map((product) => (
            <article className={styles.boutiqueProductCard} key={product.productId}>
              <Link className={styles.boutiqueProductImage} href={`/products/${product.slug}`}>
                <img src={product.mainImage} alt={product.productName} />
              </Link>
              <div className={styles.boutiqueProductInfo}>
                <h3>
                  <Link href={`/products/${product.slug}`}>{product.productName}</Link>
                </h3>
                <Money valueCent={product.salePriceCent} currency={product.currency} />
                <p>{product.subTitle}</p>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className={styles.boutiqueEditorial}>
        <div className={styles.boutiqueEditorialImage}>
          <img src={products[5]?.mainImage || products[1]?.mainImage} alt="Jewelry craftsmanship detail" />
        </div>
        <div className={styles.boutiqueEditorialCopy}>
          <p className={styles.sectionKicker}>Made with intention</p>
          <h2>From natural material to lasting form.</h2>
          <p>
            We select each stone for character, then finish every setting by hand. Small variations are part of what
            makes your piece entirely its own.
          </p>
          <Link className={styles.boutiqueSecondary} href="/search">
            Our materials
          </Link>
        </div>
      </section>

      <section className={styles.boutiquePromises} aria-label="Service promises">
        <div className={styles.boutiquePromise}>
          <strong>Thoughtful sourcing</strong>
          <span>Materials selected for beauty and longevity</span>
        </div>
        <div className={styles.boutiquePromise}>
          <strong>Gift-ready</strong>
          <span>Signature packaging included with every order</span>
        </div>
        <div className={styles.boutiquePromise}>
          <strong>Here to help</strong>
          <span>Personal support before and after your purchase</span>
        </div>
      </section>

      <footer className={styles.previewFooter}>
        <div>
          <strong>AURELIA</strong>
          <p>Modern jewelry rooted in natural beauty, careful craft, and the pleasure of wearing things often.</p>
        </div>
        <nav className={styles.previewFooterLinks} aria-label="Footer links">
          <Link href="/search">Journal</Link>
          <Link href="/shipping-policy">Delivery</Link>
          <Link href="/refund-policy">Returns</Link>
          <Link href="/account">Client care</Link>
        </nav>
      </footer>
    </div>
  );
}
