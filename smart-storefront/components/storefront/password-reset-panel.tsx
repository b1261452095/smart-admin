"use client";

import { FormEvent, useState } from "react";
import { storefrontApi } from "../../lib/storefront-api";

export function PasswordResetPanel() {
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const email = String(form.get("email") || "");

    setLoading(true);
    setMessage("");

    try {
      await storefrontApi.forgotPassword(email);
      setMessage("If an account matches this email, reset instructions will be sent.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <form className="surface-form" onSubmit={submit}>
      <label>
        Email
        <input name="email" type="email" required autoComplete="email" />
      </label>
      <button className="primary-link form-button" type="submit" disabled={loading}>
        {loading ? "Sending..." : "Send reset link"}
      </button>
      <p className="form-message" aria-live="polite">
        {message}
      </p>
    </form>
  );
}
