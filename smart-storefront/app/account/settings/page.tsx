import { AccountShell } from "../../../components/storefront/account-shell";
import { buildMetadata } from "../../../lib/seo";
import Link from "next/link";

export const metadata = buildMetadata({
  title: "Settings",
  description: "Manage storefront account security and preferences.",
  path: "/account/settings",
  noIndex: true
});

export default function AccountSettingsPage() {
  return (
    <div className="page-shell">
      <AccountShell>
        <div className="page-heading compact">
          <p className="eyebrow">Settings</p>
          <h1>Security and preferences</h1>
          <p>Manage sign-in security and your storefront preferences.</p>
        </div>
        <div className="settings-list">
          <Link href="/forgot-password">Change password</Link>
          <button type="button">Download my data</button>
          <button type="button">Delete account</button>
        </div>
      </AccountShell>
    </div>
  );
}
