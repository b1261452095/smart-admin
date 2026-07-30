"use client";

import { AnimatePresence, motion } from "motion/react";
import Link from "next/link";
import { useEffect, useState } from "react";
import { readCart } from "../../lib/cart-storage";

export function CartLink() {
  const [count, setCount] = useState(0);

  useEffect(() => {
    function sync() {
      setCount(readCart().items.reduce((sum, item) => sum + item.quantity, 0));
    }

    sync();
    window.addEventListener("smart-cart-updated", sync);
    window.addEventListener("storage", sync);
    return () => {
      window.removeEventListener("smart-cart-updated", sync);
      window.removeEventListener("storage", sync);
    };
  }, []);

  return (
    <Link className="cart-nav-link" href="/cart">
      Cart
      <AnimatePresence mode="popLayout">
        {count ? (
          <motion.span key={count} initial={{ opacity: 0, y: -4 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 4 }}>
            {count}
          </motion.span>
        ) : null}
      </AnimatePresence>
    </Link>
  );
}
