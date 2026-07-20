import { Category, CmsBlock, Product } from "./types";

export const mockProducts: Product[] = [
  {
    productId: 1001,
    categoryId: 10,
    categoryName: "Bracelets",
    productName: "Natural Amethyst Bracelet",
    productCode: "AMETHYST-BRACELET",
    slug: "natural-amethyst-bracelet-1001",
    subTitle: "Polished stone beads with sterling silver details.",
    mainImage: "https://images.unsplash.com/photo-1515562141207-7a88fb7ce338?auto=format&fit=crop&w=1200&q=80",
    salePriceCent: 3599,
    currency: "USD",
    seoTitle: "Natural Amethyst Bracelet",
    seoDescription: "Shop a natural amethyst bracelet with a polished finish and gift-ready packaging.",
    productDetail: "A lightweight bracelet made for everyday wear, finished with careful stone selection and simple hardware.",
    shelvesFlag: true,
    publishStatus: 1
  },
  {
    productId: 1002,
    categoryId: 11,
    categoryName: "Necklaces",
    productName: "Moonstone Pendant Necklace",
    productCode: "MOONSTONE-NECKLACE",
    slug: "moonstone-pendant-necklace-1002",
    subTitle: "A minimal pendant with a soft blue flash.",
    mainImage: "https://images.unsplash.com/photo-1506630448388-4e683c67ddb0?auto=format&fit=crop&w=1200&q=80",
    salePriceCent: 4899,
    currency: "USD",
    seoTitle: "Moonstone Pendant Necklace",
    seoDescription: "A minimal moonstone pendant necklace designed for daily styling.",
    productDetail: "This pendant keeps the shape quiet and refined, with an adjustable chain for layered looks.",
    shelvesFlag: true,
    publishStatus: 1
  },
  {
    productId: 1003,
    categoryId: 12,
    categoryName: "Rings",
    productName: "Stacking Silver Ring",
    productCode: "SILVER-STACK-RING",
    slug: "stacking-silver-ring-1003",
    subTitle: "A polished silver band designed for stacking.",
    mainImage: "https://images.unsplash.com/photo-1603561596112-db1d521ec28a?auto=format&fit=crop&w=1200&q=80",
    salePriceCent: 2499,
    currency: "USD",
    seoTitle: "Stacking Silver Ring",
    seoDescription: "A polished silver stacking ring for everyday styling.",
    productDetail: "A clean, durable piece that works alone or stacked with gemstone rings.",
    shelvesFlag: true,
    publishStatus: 1
  }
];

export const mockCategories: Category[] = [
  {
    categoryId: 10,
    categoryName: "Bracelets",
    categoryCode: "bracelets",
    slug: "bracelets"
  },
  {
    categoryId: 11,
    categoryName: "Necklaces",
    categoryCode: "necklaces",
    slug: "necklaces"
  },
  {
    categoryId: 12,
    categoryName: "Rings",
    categoryCode: "rings",
    slug: "rings"
  }
];

export const mockCmsBlocks: CmsBlock[] = [
  {
    blockId: 1,
    blockType: 1,
    blockName: "home-hero",
    blockTitle: "Stone Jewelry, Made For Daily Light",
    blockSubTitle: "A calm collection of polished bracelets, necklaces, and rings with gift-ready packaging.",
    image: "https://images.unsplash.com/photo-1515562141207-7a88fb7ce338?auto=format&fit=crop&w=1800&q=80",
    linkUrl: "/collections/bracelets",
    sort: 1
  },
  {
    blockId: 2,
    blockType: 2,
    blockName: "main-navigation",
    blockTitle: "Bracelets",
    linkUrl: "/collections/bracelets",
    sort: 2
  },
  {
    blockId: 3,
    blockType: 2,
    blockName: "main-navigation",
    blockTitle: "Necklaces",
    linkUrl: "/collections/necklaces",
    sort: 3
  },
  {
    blockId: 4,
    blockType: 3,
    blockName: "featured-product",
    blockTitle: "Featured Gifts",
    productId: 1001,
    productName: "Natural Amethyst Bracelet",
    sort: 4
  }
];
