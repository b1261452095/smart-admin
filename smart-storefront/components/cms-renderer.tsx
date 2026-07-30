"use client";

import { motion, useInView } from "motion/react";
import Link from "next/link";
import { CSSProperties, useEffect, useMemo, useRef, useState } from "react";
import { CmsBlock, Product } from "../lib/types";
import { ProductCard } from "./product-card";

type CmsConfig = {
  autoplay?: boolean;
  buttonText?: string;
  categorySlug?: string;
  collectionUrl?: string;
  columns?: number;
  height?: "compact" | "tall" | "full";
  imagePosition?: "left" | "right";
  layout?: "grid" | "rail";
  limit?: number;
  poster?: string;
  textPosition?: "left" | "center" | "right";
  theme?: "light" | "dark";
  videoUrl?: string;
};

export function HomeCmsRenderer({ blocks, products }: { blocks: CmsBlock[]; products: Product[] }) {
  const orderedBlocks = useMemo(
    () => [...blocks].sort((left, right) => (left.sort || 0) - (right.sort || 0) || left.blockId - right.blockId),
    [blocks]
  );
  const navigationBlocks = orderedBlocks.filter((block) => block.blockType === 2);
  const firstNavigationId = navigationBlocks[0]?.blockId;
  const hasHero = orderedBlocks.some((block) => block.blockType === 1);

  return (
    <>
      {!hasHero ? <HeroBlock block={undefined} /> : null}
      {orderedBlocks.map((block) => {
        switch (block.blockType) {
          case 1:
            return <HeroBlock key={block.blockId} block={block} />;
          case 2:
            return block.blockId === firstNavigationId ? (
              <NavigationBlock key={`navigation-${block.blockId}`} blocks={navigationBlocks} products={products} />
            ) : null;
          case 3:
            return <FeaturedProductBlock key={block.blockId} block={block} products={products} />;
          case 4:
            return <ProductGridBlock key={block.blockId} block={block} products={products} />;
          case 5:
            return <ImageTextBlock key={block.blockId} block={block} />;
          case 6:
            return <FullImageBlock key={block.blockId} block={block} />;
          case 7:
            return null;
          case 8:
            return <VideoBlock key={block.blockId} block={block} />;
          default:
            return null;
        }
      })}
    </>
  );
}

function HeroBlock({ block }: { block?: CmsBlock }) {
  const config = parseConfig(block?.configJson);
  const textPosition = config.textPosition || "left";
  const height = config.height || "tall";

  return (
    <section className={`hero-section hero-section--${height} hero-section--text-${textPosition}`}>
      {block?.image ? (
        <motion.img
          className="hero-image"
          src={block.image}
          alt={block.blockTitle || "New collection"}
          initial={false}
        />
      ) : null}
      <div className="hero-content">
        <p className="eyebrow">The new edit</p>
        <h1>{block?.blockTitle || "Made For Your Own Rhythm"}</h1>
        <p>{block?.blockSubTitle || "Intimates and jewelry selected with equal attention."}</p>
        <Link className="primary-link" href={block?.linkUrl || "/search"}>
          {config.buttonText || "Shop the edit"}
        </Link>
      </div>
    </section>
  );
}

function ProductGridBlock({ block, products }: { block: CmsBlock; products: Product[] }) {
  const sectionRef = useRef<HTMLDivElement>(null);
  const sectionInView = useInView(sectionRef, { amount: 0.08, once: true });
  const [motionReady, setMotionReady] = useState(false);
  const config = parseConfig(block.configJson);
  const limit = clamp(config.limit || 8, 2, 16);
  const columns = clamp(config.columns || 4, 2, 5);
  const filteredProducts = config.categorySlug
    ? products.filter((product) => slugify(product.categoryName || "") === slugify(config.categorySlug || ""))
    : products;
  const visibleProducts = [...filteredProducts].sort((left, right) => right.productId - left.productId).slice(0, limit);
  const style = { "--cms-product-columns": columns } as CSSProperties;

  useEffect(() => {
    setMotionReady(true);
  }, []);

  if (!visibleProducts.length) {
    return null;
  }

  return (
    <section className="section-band product-shelf">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Selected now</p>
          <h2>{block.blockTitle || "New and Noted"}</h2>
          {block.blockSubTitle ? <p className="section-intro">{block.blockSubTitle}</p> : null}
        </div>
        <Link className="secondary-link" href={config.collectionUrl || block.linkUrl || "/search"}>
          View all
        </Link>
      </div>
      <motion.div ref={sectionRef} className="product-grid product-grid--cms" style={style} initial={false}>
        {visibleProducts.map((product, index) => (
          <motion.div
            className="product-grid-item"
            key={product.productId}
            initial={false}
            animate={!motionReady || sectionInView ? { opacity: 1, y: 0 } : { opacity: 0, y: 18 }}
            transition={{ delay: sectionInView ? index * 0.045 : 0, duration: 0.38 }}
          >
            <ProductCard product={product} />
          </motion.div>
        ))}
      </motion.div>
    </section>
  );
}

function NavigationBlock({ blocks, products }: { blocks: CmsBlock[]; products: Product[] }) {
  return (
    <section className="section-band cms-category-band">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Shop by mood</p>
          <h2>Find your edit</h2>
        </div>
      </div>
      <div className="cms-category-grid">
        {blocks.map((block, index) => {
          const fallbackImage = products[index % Math.max(products.length, 1)]?.mainImage;
          return (
            <Link className="cms-category-card" key={block.blockId} href={block.linkUrl || "/search"}>
              <div className="cms-category-media">
                {block.image || fallbackImage ? (
                  <img src={block.image || fallbackImage} alt={block.blockTitle || block.blockName || "Collection"} loading="lazy" />
                ) : null}
              </div>
              <div className="cms-category-copy">
                <h3>{block.blockTitle || block.blockName || "Collection"}</h3>
                {block.blockSubTitle ? <p>{block.blockSubTitle}</p> : null}
                <span>Explore</span>
              </div>
            </Link>
          );
        })}
      </div>
    </section>
  );
}

function FeaturedProductBlock({ block, products }: { block: CmsBlock; products: Product[] }) {
  const product = products.find((item) => item.productId === block.productId) || products[0];
  const config = parseConfig(block.configJson);

  if (!product) {
    return null;
  }

  return (
    <section className={`cms-featured-product cms-featured-product--${config.imagePosition || "left"}`}>
      <div className="cms-featured-media">
        {block.image || product.mainImage ? (
          <img src={block.image || product.mainImage} alt={block.blockTitle || product.productName} loading="lazy" />
        ) : null}
      </div>
      <div className="cms-featured-copy">
        <p className="eyebrow">{product.categoryName || "The edit"}</p>
        <h2>{block.blockTitle || product.productName}</h2>
        <p>{block.blockSubTitle || product.subTitle}</p>
        <Link className="secondary-link" href={block.linkUrl || `/products/${product.slug}`}>
          {config.buttonText || "View the piece"}
        </Link>
      </div>
    </section>
  );
}

function ImageTextBlock({ block }: { block: CmsBlock }) {
  const config = parseConfig(block.configJson);
  const imagePosition = config.imagePosition || "left";

  return (
    <section className={`cms-image-text cms-image-text--${imagePosition}`}>
      <div className="cms-image-text__media">
        {block.image ? <img src={block.image} alt={block.blockTitle || "Editorial story"} loading="lazy" /> : null}
      </div>
      <div className="cms-image-text__copy">
        <p className="eyebrow">Our point of view</p>
        <h2>{block.blockTitle || "Designed from the first layer outward"}</h2>
        {block.blockSubTitle ? <p>{block.blockSubTitle}</p> : null}
        <Link className="secondary-link" href={block.linkUrl || "/search"}>
          {config.buttonText || "Discover more"}
        </Link>
      </div>
    </section>
  );
}

function FullImageBlock({ block }: { block: CmsBlock }) {
  const config = parseConfig(block.configJson);

  return (
    <section className={`cms-full-image cms-full-image--${config.height || "tall"}`}>
      {block.image ? <img src={block.image} alt={block.blockTitle || "Featured edit"} loading="lazy" /> : null}
      <div className="cms-full-image__copy">
        <p className="eyebrow">The evening edit</p>
        <h2>{block.blockTitle || "A quieter kind of statement"}</h2>
        {block.blockSubTitle ? <p>{block.blockSubTitle}</p> : null}
        <Link className="primary-link" href={block.linkUrl || "/search"}>
          {config.buttonText || "Explore the edit"}
        </Link>
      </div>
    </section>
  );
}

function VideoBlock({ block }: { block: CmsBlock }) {
  const config = parseConfig(block.configJson);

  if (!config.videoUrl) {
    return null;
  }

  return (
    <section className="cms-video">
      <video
        controls={!config.autoplay}
        autoPlay={Boolean(config.autoplay)}
        muted={Boolean(config.autoplay)}
        loop={Boolean(config.autoplay)}
        playsInline
        poster={config.poster || block.image}
      >
        <source src={config.videoUrl} />
      </video>
      {block.blockTitle ? (
        <div className="cms-video__copy">
          <h2>{block.blockTitle}</h2>
          {block.blockSubTitle ? <p>{block.blockSubTitle}</p> : null}
        </div>
      ) : null}
    </section>
  );
}

function parseConfig(configJson?: string): CmsConfig {
  if (!configJson) {
    return {};
  }

  try {
    return JSON.parse(configJson) as CmsConfig;
  } catch {
    return {};
  }
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function slugify(value: string) {
  return value.trim().toLowerCase().replace(/\s+/g, "-");
}
