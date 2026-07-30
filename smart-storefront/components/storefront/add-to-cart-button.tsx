"use client";

import { AnimatePresence, motion } from "motion/react";
import Link from "next/link";
import { useState } from "react";
import { addProductToCart } from "../../lib/cart-storage";
import { Product } from "../../lib/types";

export function AddToCartButton({ product }: { product: Product }) {
  const [added, setAdded] = useState(false);

  return (
    <div className="buy-box">
      <motion.button
        className="primary-link form-button"
        type="button"
        whileTap={{ y: 1 }}
        onClick={() => {
          addProductToCart(product, 1);
          setAdded(true);
        }}
      >
        {added ? "Added to cart" : "Add to cart"}
      </motion.button>
      <AnimatePresence>
        {added ? (
          <motion.p className="form-message" initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
            Ready when you are. <Link href="/cart">View cart</Link>
          </motion.p>
        ) : null}
      </AnimatePresence>
    </div>
  );
}
