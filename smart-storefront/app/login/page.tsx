import Link from "next/link";
import { AuthPanel } from "../../components/storefront/auth-panel";
import { buildMetadata } from "../../lib/seo";

export const metadata = buildMetadata({
  title: "Sign in",
  description: "Sign in to view orders, addresses, and saved account details.",
  path: "/login",
  noIndex: true
});

export default function LoginPage() {
  return (
    <div className="page-shell auth-page">
      <div className="page-heading">
        <p className="eyebrow">Account</p>
        <h1>Sign in</h1>
        <p>Access your orders, addresses, and account preferences.</p>
      </div>
      <AuthPanel mode="login" />
      <p className="auth-switch">
        New here? <Link href="/register">Create an account</Link> · <Link href="/forgot-password">Forgot password?</Link>
      </p>
    </div>
  );
}
