"use client";

import { Cart, CartItem, Product } from "./types";

const CART_KEY = "smart_storefront_cart";

export function createEmptyCart(): Cart {
  return {
    cartId: `cart_${Date.now()}`,
    items: []
  };
}

export function readCart(): Cart {
  if (typeof window === "undefined") {
    return createEmptyCart();
  }

  const raw = window.localStorage.getItem(CART_KEY);
  if (!raw) {
    return createEmptyCart();
  }

  try {
    const cart = JSON.parse(raw) as Cart;
    return cart.cartId && Array.isArray(cart.items) ? cart : createEmptyCart();
  } catch {
    return createEmptyCart();
  }
}

export function writeCart(cart: Cart) {
  window.localStorage.setItem(CART_KEY, JSON.stringify(cart));
  window.dispatchEvent(new CustomEvent("smart-cart-updated", { detail: cart }));
}

export function addProductToCart(product: Product, quantity = 1) {
  const cart = readCart();
  const existing = cart.items.find((item) => item.productId === product.productId);

  if (existing) {
    existing.quantity += quantity;
  } else {
    const item: CartItem = {
      lineId: `line_${product.productId}_${Date.now()}`,
      productId: product.productId,
      productName: product.productName,
      slug: product.slug,
      image: product.mainImage,
      quantity,
      salePriceCent: product.salePriceCent,
      currency: product.currency
    };
    cart.items.push(item);
  }

  writeCart(cart);
  return cart;
}

export function updateCartLine(lineId: string, quantity: number) {
  const cart = readCart();
  cart.items = cart.items
    .map((item) => (item.lineId === lineId ? { ...item, quantity } : item))
    .filter((item) => item.quantity > 0);
  writeCart(cart);
  return cart;
}

export function clearCart() {
  const cart = createEmptyCart();
  writeCart(cart);
  return cart;
}
