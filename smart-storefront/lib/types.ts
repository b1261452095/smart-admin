export type SmartResponse<T> = {
  code: number;
  ok: boolean;
  msg?: string;
  data: T;
};

export type PageResult<T> = {
  pageNum: number;
  pageSize: number;
  total: number;
  pages: number;
  list: T[];
  emptyFlag?: boolean;
};

export type CmsBlock = {
  blockId: number;
  tenantId?: number;
  blockType: 1 | 2 | 3 | number;
  blockName?: string;
  blockTitle?: string;
  blockSubTitle?: string;
  image?: string;
  linkUrl?: string;
  productId?: number;
  productName?: string;
  configJson?: string;
  sort?: number;
};

export type Category = {
  categoryId: number;
  parentId?: number;
  categoryName: string;
  categoryCode?: string;
  categoryImage?: string;
  slug: string;
  children?: Category[];
};

export type Product = {
  productId: number;
  categoryId?: number;
  categoryName?: string;
  productName: string;
  productCode?: string;
  slug: string;
  subTitle?: string;
  mainImage?: string;
  detailImages?: string;
  salePriceCent: number;
  currency: string;
  seoTitle?: string;
  seoDescription?: string;
  productDetail?: string;
  shelvesFlag?: boolean;
  publishStatus?: number;
};
