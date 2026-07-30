import { mockCategories, mockCmsBlocks, mockProducts } from "./mock-data";
import { Category, CmsBlock, PageResult, Product, SmartResponse } from "./types";
import { extractIdFromSlug, getApiBaseUrl, getTenantId, makeCategorySlug, makeProductSlug } from "./url";

const REVALIDATE_SECONDS = 300;

type RequestOptions<T> = {
  fallback: T;
  method?: "GET" | "POST";
  body?: unknown;
};

async function requestSmartAdmin<T>(path: string, options: RequestOptions<T>): Promise<T> {
  const url = `${getApiBaseUrl()}${path}`;

  try {
    const response = await fetch(url, {
      method: options.method || "GET",
      headers: {
        "content-type": "application/json"
      },
      body: options.body ? JSON.stringify(options.body) : undefined,
      next: {
        revalidate: REVALIDATE_SECONDS
      }
    });

    if (!response.ok) {
      return options.fallback;
    }

    const payload = (await response.json()) as SmartResponse<T> | T;

    if (isSmartResponse(payload)) {
      return payload.ok && payload.data ? payload.data : options.fallback;
    }

    return payload;
  } catch {
    return options.fallback;
  }
}

function isSmartResponse<T>(payload: SmartResponse<T> | T): payload is SmartResponse<T> {
  return typeof payload === "object" && payload !== null && "ok" in payload && "data" in payload;
}

function normalizeProduct(product: Partial<Product>): Product {
  const productId = Number(product.productId || 0);
  const productName = product.productName || "Untitled Product";

  return {
    productId,
    categoryId: product.categoryId,
    categoryName: product.categoryName,
    productName,
    productCode: product.productCode,
    slug: product.slug || makeProductSlug(productName, productId, product.productCode),
    subTitle: product.subTitle,
    mainImage: product.mainImage,
    detailImages: product.detailImages,
    salePriceCent: Number(product.salePriceCent || 0),
    currency: product.currency || "USD",
    seoTitle: product.seoTitle,
    seoDescription: product.seoDescription,
    productDetail: product.productDetail,
    shelvesFlag: product.shelvesFlag,
    publishStatus: product.publishStatus
  };
}

function normalizeCategory(category: Partial<Category>): Category {
  const categoryId = Number(category.categoryId || 0);
  const categoryName = category.categoryName || "Collection";

  return {
    categoryId,
    parentId: category.parentId,
    categoryName,
    categoryCode: category.categoryCode,
    categoryImage: category.categoryImage,
    slug: category.slug || makeCategorySlug(categoryName, categoryId, category.categoryCode),
    children: category.children?.map(normalizeCategory)
  };
}

function normalizeCmsBlock(block: CmsBlock & { image?: unknown }): CmsBlock {
  const image = Array.isArray(block.image)
    ? block.image[0]?.fileUrl || block.image[0]?.url || ""
    : typeof block.image === "string"
      ? block.image
      : "";

  return {
    ...block,
    blockId: Number(block.blockId || 0),
    blockType: Number(block.blockType || 0),
    productId: block.productId ? Number(block.productId) : undefined,
    image,
    sort: Number(block.sort || 0),
    version: Number(block.version || 0)
  };
}

export async function getCmsBlocks(blockType?: number) {
  const fallback = blockType ? mockCmsBlocks.filter((block) => block.blockType === blockType) : mockCmsBlocks;

  const blocks = await requestSmartAdmin<CmsBlock[]>("/shop/client/cms/block/list", {
    method: "POST",
    body: {
      tenantId: getTenantId(),
      blockType
    },
    fallback
  });

  return blocks
    .map(normalizeCmsBlock)
    .filter((block) => !block.disabledFlag)
    .sort((left, right) => (left.sort || 0) - (right.sort || 0) || left.blockId - right.blockId);
}

export async function getCategories() {
  const categories = await requestSmartAdmin<Category[]>("/shop/client/category/tree", {
    method: "POST",
    body: {
      tenantId: getTenantId()
    },
    fallback: mockCategories
  });

  return categories.map(normalizeCategory);
}

export async function getProducts(options?: { keyword?: string; categorySlug?: string; limit?: number }) {
  const pageSize = options?.limit || 12;
  const fallback = filterMockProducts(options);

  const pageResult = await requestSmartAdmin<PageResult<Product>>("/shop/client/product/queryPage", {
    method: "POST",
    body: {
      tenantId: getTenantId(),
      pageNum: 1,
      pageSize,
      keyword: options?.keyword,
      categorySlug: options?.categorySlug,
      shelvesFlag: true,
      publishStatus: 1
    },
    fallback: {
      pageNum: 1,
      pageSize,
      total: fallback.length,
      pages: 1,
      list: fallback
    }
  });

  return pageResult.list.map(normalizeProduct);
}

export async function getProductBySlug(slug: string) {
  const fallback = mockProducts.find((product) => product.slug === slug) || null;
  const product = await requestSmartAdmin<Product | null>(`/shop/client/product/getBySlug/${slug}`, {
    fallback
  });

  if (product) {
    return normalizeProduct(product);
  }

  const productId = extractIdFromSlug(slug);
  if (!productId) {
    return fallback;
  }

  const idProduct = await requestSmartAdmin<Product | null>(`/shop/client/product/get/${productId}`, {
    fallback
  });

  return idProduct ? normalizeProduct(idProduct) : fallback;
}

function filterMockProducts(options?: { keyword?: string; categorySlug?: string; limit?: number }) {
  const keyword = options?.keyword?.trim().toLowerCase();
  const list = mockProducts.filter((product) => {
    const matchesKeyword = keyword
      ? [product.productName, product.subTitle, product.categoryName].some((value) => value?.toLowerCase().includes(keyword))
      : true;
    const matchesCategory = options?.categorySlug ? product.categoryName?.toLowerCase() === options.categorySlug : true;

    return matchesKeyword && matchesCategory;
  });

  return list.slice(0, options?.limit || list.length);
}
