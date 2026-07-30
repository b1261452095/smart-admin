"use client";

export type StorefrontLocale = "en-US" | "de" | "fr" | "es";
export type StorefrontCurrency = "USD" | "EUR" | "GBP" | "CAD" | "AUD";

export type CommercePreferences = {
  locale: StorefrontLocale;
  currency: StorefrontCurrency;
};

const PREFERENCE_KEY = "smart_storefront_preferences";

const currencyRates: Record<StorefrontCurrency, number> = {
  USD: 1,
  EUR: 0.92,
  GBP: 0.78,
  CAD: 1.36,
  AUD: 1.52
};

export const supportedLocales: Array<{ value: StorefrontLocale; label: string }> = [
  { value: "en-US", label: "English" },
  { value: "de", label: "Deutsch" },
  { value: "fr", label: "Français" },
  { value: "es", label: "Español" }
];

export const supportedCurrencies: Array<{ value: StorefrontCurrency; label: string }> = [
  { value: "USD", label: "USD" },
  { value: "EUR", label: "EUR" },
  { value: "GBP", label: "GBP" },
  { value: "CAD", label: "CAD" },
  { value: "AUD", label: "AUD" }
];

export function defaultPreferences(): CommercePreferences {
  return {
    locale: "en-US",
    currency: "USD"
  };
}

export function readPreferences(): CommercePreferences {
  if (typeof window === "undefined") {
    return defaultPreferences();
  }

  const raw = window.localStorage.getItem(PREFERENCE_KEY);
  if (!raw) {
    return defaultPreferences();
  }

  try {
    return { ...defaultPreferences(), ...(JSON.parse(raw) as Partial<CommercePreferences>) };
  } catch {
    return defaultPreferences();
  }
}

export function writePreferences(preferences: CommercePreferences) {
  window.localStorage.setItem(PREFERENCE_KEY, JSON.stringify(preferences));
  window.document.cookie = `lang_pref=${preferences.locale}; path=/; max-age=31536000; samesite=lax`;
  window.document.cookie = `currency_pref=${preferences.currency}; path=/; max-age=31536000; samesite=lax`;
  window.dispatchEvent(new CustomEvent("smart-preferences-updated", { detail: preferences }));
}

export function convertCent(valueCent: number, fromCurrency: string, toCurrency: StorefrontCurrency) {
  if (fromCurrency === toCurrency) {
    return valueCent;
  }

  const baseUsdCent = fromCurrency === "USD" ? valueCent : Math.round(valueCent / (currencyRates[fromCurrency as StorefrontCurrency] || 1));
  return Math.round(baseUsdCent * currencyRates[toCurrency]);
}
