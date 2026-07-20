import Link from "next/link";
import { CmsBlock, Product } from "../lib/types";
import { ProductCard } from "./product-card";

export function HomeCmsRenderer({ blocks, products }: { blocks: CmsBlock[]; products: Product[] }) {
  const hero = blocks.find((block) => block.blockType === 1);
  const navBlocks = blocks.filter((block) => block.blockType === 2);
  const featuredBlocks = blocks.filter((block) => block.blockType === 3);
  const featuredProducts = featuredBlocks.length
    ? products.filter((product) => featuredBlocks.some((block) => block.productId === product.productId))
    : products.slice(0, 3);

  return (
    <>
      <section className="hero-section">
        {hero?.image ? <img className="hero-image" src={hero.image} alt={hero.blockTitle || "Storefront hero"} /> : null}
        <div className="hero-content">
          <p className="eyebrow">New collection</p>
          <h1>{hero?.blockTitle || "A Storefront Built For Search And Sales"}</h1>
          <p>{hero?.blockSubTitle || "Browse curated products, fast pages, and server-rendered content ready for organic traffic."}</p>
          <Link className="primary-link" href={hero?.linkUrl || "/search"}>
            Shop now
          </Link>
        </div>
      </section>

      {navBlocks.length ? (
        <section className="section-band">
          <div className="section-heading">
            <p className="eyebrow">Browse</p>
            <h2>Shop by collection</h2>
          </div>
          <div className="collection-links">
            {navBlocks.map((block) => (
              <Link key={block.blockId} href={block.linkUrl || "/search"}>
                {block.blockTitle || block.blockName || "Collection"}
              </Link>
            ))}
          </div>
        </section>
      ) : null}

      <section className="section-band">
        <div className="section-heading">
          <p className="eyebrow">Recommended</p>
          <h2>{featuredBlocks[0]?.blockTitle || "Popular picks"}</h2>
        </div>
        <div className="product-grid">
          {featuredProducts.map((product) => (
            <ProductCard key={product.productId} product={product} />
          ))}
        </div>
      </section>
    </>
  );
}
