import { ProductCard } from "../../components/product-card";
import { getProducts } from "../../lib/api";
import { buildMetadata } from "../../lib/seo";

export const revalidate = 300;

type SearchPageProps = {
  searchParams?: {
    q?: string;
  };
};

export const metadata = buildMetadata({
  title: "Search",
  description: "Search the current jewelry collection.",
  path: "/search"
});

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const query = searchParams?.q?.trim() || "";
  const products = await getProducts({ keyword: query, limit: 24 });

  return (
    <div className="page-shell">
      <div className="page-heading">
        <p className="eyebrow">Search</p>
        <h1>{query ? `Results for ${query}` : "Search products"}</h1>
        <p>Search by piece, material, or collection.</p>
      </div>
      <form className="search-form" action="/search">
        <input name="q" type="search" defaultValue={query} placeholder="Search bracelets, necklaces, rings" />
        <button type="submit">Search</button>
      </form>
      {products.length ? (
        <div className="product-grid">
          {products.map((product) => (
            <ProductCard key={product.productId} product={product} />
          ))}
        </div>
      ) : (
        <div className="empty-state">
          <h2>No matches</h2>
          <p>Try a broader search or browse the full collection.</p>
        </div>
      )}
    </div>
  );
}
