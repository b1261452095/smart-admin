"use client";

import Link from "next/link";
import { FormEvent, useEffect, useMemo, useState } from "react";
import { clearCart, readCart } from "../../lib/cart-storage";
import { storefrontApi } from "../../lib/storefront-api";
import { Address, Cart, CheckoutQuote } from "../../lib/types";
import { Money } from "./money";

export function CheckoutClient() {
  const [cart, setCart] = useState<Cart>(() => ({ cartId: "", items: [] }));
  const [quote, setQuote] = useState<CheckoutQuote | null>(null);
  const [country, setCountry] = useState("US");
  const [message, setMessage] = useState("");

  useEffect(() => {
    setCart(readCart());
  }, []);

  useEffect(() => {
    if (!cart.items.length) {
      return;
    }

    const timer = window.setTimeout(async () => {
      const nextQuote = await storefrontApi.quote(cart, { country });
      setQuote(nextQuote);
    }, 500);

    return () => window.clearTimeout(timer);
  }, [cart, country]);

  const currency = quote?.currency || cart.items[0]?.currency || "USD";
  const itemCount = useMemo(() => cart.items.reduce((sum, item) => sum + item.quantity, 0), [cart]);

  async function placeOrder(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const address: Address = {
      country,
      firstName: String(form.get("firstName") || ""),
      lastName: String(form.get("lastName") || ""),
      address1: String(form.get("address1") || ""),
      address2: String(form.get("address2") || ""),
      city: String(form.get("city") || ""),
      state: String(form.get("state") || ""),
      zip: String(form.get("zip") || ""),
      phone: String(form.get("phone") || "")
    };

    await storefrontApi.quote(cart, address);
    clearCart();
    setMessage("Order placed.");
    window.location.href = `/checkout/success?order=demo-${Date.now()}`;
  }

  if (!cart.items.length) {
    return (
      <div className="empty-state">
        <h2>No items to checkout</h2>
        <p>Add a piece to your cart before continuing.</p>
        <Link className="primary-link" href="/search">
          Browse products
        </Link>
      </div>
    );
  }

  return (
    <form className="checkout-layout" onSubmit={placeOrder}>
      <div className="checkout-form">
        <section className="checkout-section">
          <p className="eyebrow">Contact</p>
          <label>
            Email
            <input name="email" type="email" required autoComplete="email" />
          </label>
          <label className="check-row">
            <input name="createAccount" type="checkbox" />
            <span>Create an account after payment</span>
          </label>
        </section>

        <section className="checkout-section">
          <p className="eyebrow">Shipping address</p>
          <label>
            Country
            <select name="country" value={country} onChange={(event) => setCountry(event.target.value)}>
              <option value="US">United States</option>
              <option value="DE">Germany</option>
              <option value="GB">United Kingdom</option>
            </select>
          </label>
          <div className="form-grid two">
            <label>
              First name
              <input name="firstName" required autoComplete="given-name" />
            </label>
            <label>
              Last name
              <input name="lastName" required autoComplete="family-name" />
            </label>
          </div>
          <label>
            Address
            <input name="address1" required autoComplete="address-line1" />
          </label>
          <label>
            Apt, suite
            <input name="address2" autoComplete="address-line2" />
          </label>
          <div className="form-grid three">
            <label>
              City
              <input name="city" required autoComplete="address-level2" />
            </label>
            <label>
              State
              <input name="state" autoComplete="address-level1" />
            </label>
            <label>
              Zip
              <input name="zip" required autoComplete="postal-code" />
            </label>
          </div>
          <label>
            Phone
            <input name="phone" required autoComplete="tel" />
          </label>
        </section>

        <section className="checkout-section">
          <p className="eyebrow">Payment</p>
          <div className="payment-placeholder">
            Payment details will appear here when the checkout connection is ready.
          </div>
        </section>
      </div>

      <aside className="summary-panel checkout-summary">
        <p className="eyebrow">Order summary</p>
        <h2>{itemCount} items</h2>
        {cart.items.map((item) => (
          <div className="summary-row" key={item.lineId}>
            <span>{item.productName}</span>
            <strong>
              <Money valueCent={item.salePriceCent * item.quantity} currency={item.currency} />
            </strong>
          </div>
        ))}
        <div className="summary-row">
          <span>Shipping</span>
          <strong>{quote ? <Money valueCent={quote.shippingCent} currency={currency} /> : "..."}</strong>
        </div>
        <div className="summary-row">
          <span>Tax</span>
          <strong>{quote ? <Money valueCent={quote.taxCent} currency={currency} /> : "..."}</strong>
        </div>
        <div className="summary-row total">
          <span>Total</span>
          <strong>{quote ? <Money valueCent={quote.totalCent} currency={currency} /> : "..."}</strong>
        </div>
        <button className="primary-link form-button" type="submit">
          Pay {quote ? <Money valueCent={quote.totalCent} currency={currency} /> : ""}
        </button>
        <p>Your final total includes the shipping and tax shown above.</p>
        {message ? <p className="form-message">{message}</p> : null}
      </aside>
    </form>
  );
}
