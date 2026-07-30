import { AccountShell } from "../../../components/storefront/account-shell";
import { buildMetadata } from "../../../lib/seo";

export const metadata = buildMetadata({
  title: "Addresses",
  description: "Manage storefront shipping addresses.",
  path: "/account/addresses",
  noIndex: true
});

export default function AccountAddressesPage() {
  return (
    <div className="page-shell">
      <AccountShell>
        <div className="page-heading compact">
          <p className="eyebrow">Addresses</p>
          <h1>Address book</h1>
          <p>Save delivery details for a quicker checkout.</p>
        </div>
        <form className="surface-form inline-form">
          <div className="form-grid two">
            <label>
              First name
              <input />
            </label>
            <label>
              Last name
              <input />
            </label>
          </div>
          <label>
            Address
            <input />
          </label>
          <div className="form-grid three">
            <label>
              Country
              <input defaultValue="US" />
            </label>
            <label>
              City
              <input />
            </label>
            <label>
              Zip
              <input />
            </label>
          </div>
          <button className="primary-link form-button" type="button">
            Save address
          </button>
        </form>
      </AccountShell>
    </div>
  );
}
