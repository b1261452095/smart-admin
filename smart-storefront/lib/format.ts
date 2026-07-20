export function formatMoney(valueCent: number, currency = "USD") {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency
  }).format((valueCent || 0) / 100);
}
