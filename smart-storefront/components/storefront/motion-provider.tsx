"use client";

import { MotionConfig } from "motion/react";
import { ReactNode } from "react";

export function StorefrontMotionProvider({ children }: { children: ReactNode }) {
  return (
    <MotionConfig reducedMotion="user" transition={{ duration: 0.24, ease: [0.16, 1, 0.3, 1] }}>
      {children}
    </MotionConfig>
  );
}
