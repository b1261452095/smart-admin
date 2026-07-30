"use client";

import { useEffect, useState } from "react";
import { convertCent, readPreferences, StorefrontCurrency, StorefrontLocale } from "../../lib/commerce-preferences";

type MoneyProps = {
  valueCent: number;
  currency?: string;
  className?: string;
};

export function Money({ valueCent, currency = "USD", className }: MoneyProps) {
  const [targetCurrency, setTargetCurrency] = useState<StorefrontCurrency>("USD");
  const [locale, setLocale] = useState<StorefrontLocale>("en-US");

  useEffect(() => {
    function sync() {
      const preferences = readPreferences();
      setTargetCurrency(preferences.currency);
      setLocale(preferences.locale);
    }

    sync();
    window.addEventListener("smart-preferences-updated", sync);
    return () => window.removeEventListener("smart-preferences-updated", sync);
  }, []);

  const displayCent = convertCent(valueCent, currency, targetCurrency);
  const formatted = new Intl.NumberFormat(locale, {
    style: "currency",
    currency: targetCurrency
  }).format(displayCent / 100);

  return <span className={className}>{formatted}</span>;
}
