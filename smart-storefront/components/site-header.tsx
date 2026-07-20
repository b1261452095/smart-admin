import Link from "next/link";
import { Category } from "../lib/types";
import { getStoreName } from "../lib/url";

export function SiteHeader({ categories }: { categories: Category[] }) {
  return (
    <header className="site-header">
      <Link className="brand" href="/" aria-label={`${getStoreName()} home`}>
        {getStoreName()}
      </Link>
      <nav className="main-nav" aria-label="Main navigation">
        {categories.slice(0, 5).map((category) => (
          <Link key={category.categoryId} href={`/collections/${category.slug}`}>
            {category.categoryName}
          </Link>
        ))}
        <Link href="/search">Search</Link>
      </nav>
    </header>
  );
}
