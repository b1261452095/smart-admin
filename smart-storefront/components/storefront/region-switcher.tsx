"use client";

import { useEffect, useState } from "react";
import {
  CommercePreferences,
  defaultPreferences,
  readPreferences,
  supportedCurrencies,
  supportedLocales,
  writePreferences
} from "../../lib/commerce-preferences";

export function RegionSwitcher() {
  const [preferences, setPreferences] = useState<CommercePreferences>(defaultPreferences());

  useEffect(() => {
    setPreferences(readPreferences());
  }, []);

  function update(next: Partial<CommercePreferences>) {
    const merged = { ...preferences, ...next };
    setPreferences(merged);
    writePreferences(merged);
  }

  return (
    <div className="region-switcher" aria-label="Language and currency">
      <select value={preferences.locale} onChange={(event) => update({ locale: event.target.value as CommercePreferences["locale"] })}>
        {supportedLocales.map((locale) => (
          <option key={locale.value} value={locale.value}>
            {locale.label}
          </option>
        ))}
      </select>
      <select value={preferences.currency} onChange={(event) => update({ currency: event.target.value as CommercePreferences["currency"] })}>
        {supportedCurrencies.map((currency) => (
          <option key={currency.value} value={currency.value}>
            {currency.label}
          </option>
        ))}
      </select>
    </div>
  );
}
