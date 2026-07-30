"use client";

import { FormEvent, useState } from "react";
import { storefrontApi } from "../../lib/storefront-api";

type AuthPanelProps = {
  mode: "login" | "register";
};

export function AuthPanel({ mode }: AuthPanelProps) {
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const email = String(form.get("email") || "");
    const password = String(form.get("password") || "");

    setLoading(true);
    setMessage("");

    try {
      const result =
        mode === "register"
          ? await storefrontApi.register({
              email,
              password,
              firstName: String(form.get("firstName") || ""),
              lastName: String(form.get("lastName") || ""),
              newsletterFlag: form.get("newsletterFlag") === "on"
            })
          : await storefrontApi.login({
              account: email,
              password,
              rememberMe: form.get("rememberMe") === "on"
            });

      window.localStorage.setItem("smart_storefront_customer", JSON.stringify(result.customer));
      window.localStorage.setItem("smart_storefront_token", result.tokens.accessToken);
      setMessage(mode === "register" ? "Account created. You can now sign in." : "Welcome back.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <form className="surface-form" onSubmit={submit}>
      {mode === "register" ? (
        <div className="form-grid two">
          <label>
            First name
            <input name="firstName" autoComplete="given-name" />
          </label>
          <label>
            Last name
            <input name="lastName" autoComplete="family-name" />
          </label>
        </div>
      ) : null}

      <label>
        Email
        <input name="email" type="email" required autoComplete="email" />
      </label>
      <label>
        Password
        <input name="password" type="password" required minLength={8} autoComplete={mode === "register" ? "new-password" : "current-password"} />
      </label>

      {mode === "register" ? (
        <>
          <label className="check-row">
            <input name="agree" type="checkbox" required />
            <span>I agree to the privacy policy.</span>
          </label>
          <label className="check-row">
            <input name="newsletterFlag" type="checkbox" />
            <span>Subscribe to product updates.</span>
          </label>
        </>
      ) : (
        <label className="check-row">
          <input name="rememberMe" type="checkbox" />
          <span>Remember me</span>
        </label>
      )}

      <button className="primary-link form-button" type="submit" disabled={loading}>
        {loading ? "Working..." : mode === "register" ? "Create account" : "Sign in"}
      </button>
      <p className="form-message" aria-live="polite">
        {message}
      </p>
    </form>
  );
}
