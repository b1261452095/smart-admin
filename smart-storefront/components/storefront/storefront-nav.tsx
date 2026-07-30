"use client";

import { AnimatePresence, motion } from "motion/react";
import Link from "next/link";
import { useEffect, useState } from "react";
import { Category } from "../../lib/types";
import { CartLink } from "./cart-link";

type StorefrontNavProps = {
  announcement?: string;
  announcementHref?: string;
  categories: Category[];
  storeName: string;
};

export function StorefrontNav({ announcement, announcementHref, categories, storeName }: StorefrontNavProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [hidden, setHidden] = useState(false);

  useEffect(() => {
    let previousY = window.scrollY;

    function handleScroll() {
      const nextY = window.scrollY;
      setHidden(nextY > previousY && nextY > 120);
      previousY = nextY;
    }

    window.addEventListener("scroll", handleScroll, { passive: true });
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  useEffect(() => {
    setHidden(false);
  }, [menuOpen]);

  const categoryLinks = categories.slice(0, 8);

  return (
    <motion.header
      className="site-header"
      animate={{ y: hidden ? "-100%" : "0%" }}
      transition={{ duration: 0.28, ease: [0.65, 0, 0.35, 1] }}
    >
      {announcement ? (
        <Link className="announcement-bar" href={announcementHref || "/search"}>
          <span>{announcement}</span>
          <span aria-hidden="true">Discover</span>
        </Link>
      ) : null}

      <div className="header-core">
        <button
          className="menu-toggle"
          type="button"
          aria-expanded={menuOpen}
          aria-controls="mobile-navigation"
          onClick={() => setMenuOpen((open) => !open)}
        >
          {menuOpen ? "Close" : "Menu"}
        </button>
        <nav className="utility-nav utility-nav-left" aria-label="Primary navigation">
          <Link href="/search">New in</Link>
          {categoryLinks.slice(0, 3).map((category) => (
            <Link key={category.categoryId} href={`/collections/${category.slug}`}>
              {category.categoryName}
            </Link>
          ))}
        </nav>
        <Link className="brand" href="/" aria-label={`${storeName} home`}>
          {storeName}
        </Link>
        <nav className="utility-nav utility-nav-right" aria-label="Customer navigation">
          <Link href="/search">Search</Link>
          <Link href="/account">Account</Link>
          <CartLink />
        </nav>
        <div className="mobile-cart-link">
          <CartLink />
        </div>
      </div>

      <nav className="category-nav" aria-label="Collections">
        {categoryLinks.map((category) => (
          <Link key={category.categoryId} href={`/collections/${category.slug}`}>
            {category.categoryName}
          </Link>
        ))}
      </nav>

      <AnimatePresence>
        {menuOpen ? (
          <motion.nav
            id="mobile-navigation"
            className="mobile-navigation"
            aria-label="Mobile navigation"
            initial={{ opacity: 0, y: -12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -12 }}
          >
            <Link href="/search" onClick={() => setMenuOpen(false)}>
              Shop all
            </Link>
            {categoryLinks.map((category) => (
              <Link key={category.categoryId} href={`/collections/${category.slug}`} onClick={() => setMenuOpen(false)}>
                {category.categoryName}
              </Link>
            ))}
            <Link href="/account" onClick={() => setMenuOpen(false)}>
              Account
            </Link>
          </motion.nav>
        ) : null}
      </AnimatePresence>
    </motion.header>
  );
}
