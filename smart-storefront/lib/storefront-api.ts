import { Address, AuthTokens, Cart, CheckoutQuote, Customer, OrderPreview, SmartResponse } from "./types";
import { getApiBaseUrl, getTenantId } from "./url";

type ApiOptions<T> = {
  fallback: T;
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  body?: unknown;
  token?: string;
};

export async function storefrontRequest<T>(path: string, options: ApiOptions<T>): Promise<T> {
  try {
    const response = await fetch(`${getApiBaseUrl()}${path}`, {
      method: options.method || "GET",
      headers: {
        "content-type": "application/json",
        ...(options.token ? { "x-shop-token": options.token } : {})
      },
      body: options.body ? JSON.stringify({ tenantId: getTenantId(), ...asObject(options.body) }) : undefined,
      cache: "no-store"
    });

    if (!response.ok) {
      return options.fallback;
    }

    const payload = (await response.json()) as SmartResponse<T> | T;
    if (typeof payload === "object" && payload !== null && "ok" in payload && "data" in payload) {
      return payload.ok ? payload.data : options.fallback;
    }

    return payload;
  } catch {
    return options.fallback;
  }
}

function asObject(value: unknown) {
  return typeof value === "object" && value !== null ? value : {};
}

export const storefrontApi = {
  register(body: { email: string; password: string; firstName?: string; lastName?: string; newsletterFlag?: boolean }) {
    return storefrontRequest<{ customer: Customer; tokens: AuthTokens }>("/shop/client/customer/register", {
      method: "POST",
      body,
      fallback: {
        customer: {
          customerId: Date.now(),
          email: body.email,
          firstName: body.firstName,
          lastName: body.lastName,
          newsletterFlag: body.newsletterFlag
        },
        tokens: {
          accessToken: `demo-token-${Date.now()}`,
          expiresIn: 3600
        }
      }
    });
  },
  login(body: { account: string; password: string; rememberMe?: boolean }) {
    return storefrontRequest<{ customer: Customer; tokens: AuthTokens }>("/shop/client/customer/login", {
      method: "POST",
      body,
      fallback: {
        customer: {
          customerId: 1,
          email: body.account,
          firstName: "Demo"
        },
        tokens: {
          accessToken: `demo-token-${Date.now()}`,
          expiresIn: body.rememberMe ? 30 * 86400 : 86400
        }
      }
    });
  },
  forgotPassword(email: string) {
    return storefrontRequest<{ sent: boolean }>("/storefront/auth/forgot-password", {
      method: "POST",
      body: {
        email
      },
      fallback: {
        sent: true
      }
    });
  },
  quote(cart: Cart, address?: Partial<Address>) {
    const subtotalCent = cart.items.reduce((sum, item) => sum + item.salePriceCent * item.quantity, 0);
    const shippingCent = subtotalCent > 7500 ? 0 : 1200;
    const taxCent = address?.country === "DE" ? Math.round(subtotalCent * 0.19) : Math.round(subtotalCent * 0.08);
    const discountCent = cart.couponCode ? Math.round(subtotalCent * 0.1) : 0;

    return storefrontRequest<CheckoutQuote>("/storefront/checkout/quote", {
      method: "POST",
      body: {
        cart,
        shippingAddress: address
      },
      fallback: {
        subtotalCent,
        shippingCent,
        taxCent,
        discountCent,
        totalCent: subtotalCent + shippingCent + taxCent - discountCent,
        currency: cart.items[0]?.currency || "USD",
        shippingMethods: [
          {
            shippingMethodId: "standard",
            name: "Standard",
            description: "7-14 business days",
            amountCent: shippingCent
          },
          {
            shippingMethodId: "express",
            name: "Express",
            description: "3-5 business days",
            amountCent: 2800
          }
        ]
      }
    });
  },
  orders(token?: string) {
    return storefrontRequest<OrderPreview[]>("/storefront/account/orders", {
      token,
      fallback: [
        {
          orderId: 8899,
          orderNo: "EC-2026-0008899",
          status: "Shipped",
          amountCent: 10965,
          currency: "USD",
          itemCount: 3,
          createTime: "2026-07-20"
        }
      ]
    });
  }
};
