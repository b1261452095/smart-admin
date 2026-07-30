"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { readCart, updateCartLine } from "../../lib/cart-storage";
import { Cart } from "../../lib/types";
import { Money } from "./money";

export function CartClient() {
  const [cart, setCart] = useState<Cart>(() => ({ cartId: "", items: [] }));

  useEffect(() => {
    setCart(readCart());
  }, []);

  const subtotalCent = useMemo(() => cart.items.reduce((sum, item) => sum + item.salePriceCent * item.quantity, 0), [cart]);

  if (!cart.items.length) {
    return (
      <div className="empty-state">
        <h2>Your cart is empty</h2>
        <p>Explore the collection and add a piece when you are ready.</p>
        <Link className="primary-link" href="/search">
          Browse products
        </Link>
      </div>
    );
  }

  return (
    <div className="cart-layout">
      <div className="cart-lines">
        {cart.items.map((item) => (
          <article className="cart-line" key={item.lineId}>
            {item.image ? <img src={item.image} alt={item.productName} /> : <span className="image-placeholder" />}
            <div>
              <h3>
                <Link href={`/products/${item.slug}`}>{item.productName}</Link>
              </h3>
              <p>
                <Money valueCent={item.salePriceCent} currency={item.currency} />
              </p>
            </div>
            <input
              aria-label={`Quantity for ${item.productName}`}
              min={0}
              type="number"
              value={item.quantity}
              onChange={(event) => setCart(updateCartLine(item.lineId, Number(event.target.value)))}
            />
          </article>
        ))}
      </div>
      <aside className="summary-panel">
        <p className="eyebrow">Order summary</p>
        <div className="summary-row">
          <span>Subtotal</span>
          <strong>
            <Money valueCent={subtotalCent} currency={cart.items[0]?.currency || "USD"} />
          </strong>
        </div>
        <p>Shipping and tax are calculated during checkout from the shipping address.</p>
        <Link className="primary-link" href="/checkout">
          Checkout
        </Link>
      </aside>
    </div>
  );
}
