import Link from "next/link";
import { AuthPanel } from "../../components/storefront/auth-panel";
import { buildMetadata } from "../../lib/seo";

export const metadata = buildMetadata({
  title: "Create account",
  description: "Create a storefront customer account for order history and saved addresses.",
  path: "/register",
  noIndex: true
});

export default function RegisterPage() {
  return (
    <div className="page-shell auth-page">
      <div className="page-heading">
        <p className="eyebrow">Account</p>
        <h1>Create account</h1>
        <p>Keep order history and delivery details together in one place.</p>
      </div>
      <AuthPanel mode="register" />
      <p className="auth-switch">
        Already have an account? <Link href="/login">Sign in</Link>
      </p>
    </div>
  );
}
