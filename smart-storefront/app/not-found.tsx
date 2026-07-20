import Link from "next/link";

export default function NotFound() {
  return (
    <div className="page-shell">
      <div className="page-heading">
        <p className="eyebrow">404</p>
        <h1>Page not found</h1>
        <p>The page may have moved, or the product is no longer available.</p>
      </div>
      <Link className="primary-link" href="/">
        Back to home
      </Link>
    </div>
  );
}
