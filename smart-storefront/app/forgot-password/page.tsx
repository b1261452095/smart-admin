import Link from "next/link";
import { PasswordResetPanel } from "../../components/storefront/password-reset-panel";
import { buildMetadata } from "../../lib/seo";

export const metadata = buildMetadata({
  title: "Reset password",
  description: "Request a storefront account password reset link.",
  path: "/forgot-password",
  noIndex: true
});

export default function ForgotPasswordPage() {
  return (
    <div className="page-shell auth-page">
      <div className="page-heading">
        <p className="eyebrow">Account security</p>
        <h1>Reset password</h1>
        <p>Enter the email address associated with your account.</p>
      </div>
      <PasswordResetPanel />
      <p className="auth-switch">
        Remembered it? <Link href="/login">Back to sign in</Link>
      </p>
    </div>
  );
}
