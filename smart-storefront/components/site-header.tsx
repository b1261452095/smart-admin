import { Category } from "../lib/types";
import { getStoreName } from "../lib/url";
import { StorefrontNav } from "./storefront/storefront-nav";

type SiteHeaderProps = {
  announcement?: string;
  announcementHref?: string;
  categories: Category[];
};

export function SiteHeader({ announcement, announcementHref, categories }: SiteHeaderProps) {
  return (
    <StorefrontNav
      announcement={announcement}
      announcementHref={announcementHref}
      categories={categories}
      storeName={getStoreName()}
    />
  );
}
