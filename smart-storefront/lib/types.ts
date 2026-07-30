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
  blockType: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | number;
  blockName?: string;
  blockTitle?: string;
  blockSubTitle?: string;
  image?: string;
  linkUrl?: string;
  productId?: number;
  productName?: string;
  configJson?: string;
  sort?: number;
  disabledFlag?: boolean;
  version?: number;
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

export type Customer = {
  customerId: number;
  email: string;
  firstName?: string;
  lastName?: string;
  phone?: string;
  newsletterFlag?: boolean;
};

export type AuthTokens = {
  accessToken: string;
  refreshToken?: string;
  expiresIn?: number;
};

export type CartItem = {
  lineId: string;
  productId: number;
  productName: string;
  slug: string;
  image?: string;
  skuId?: number;
  skuName?: string;
  quantity: number;
  salePriceCent: number;
  currency: string;
};

export type Cart = {
  cartId: string;
  items: CartItem[];
  couponCode?: string;
};

export type Address = {
  id?: number;
  country: string;
  firstName: string;
  lastName: string;
  address1: string;
  address2?: string;
  city: string;
  state?: string;
  zip: string;
  phone: string;
  defaultShipping?: boolean;
};

export type CheckoutQuote = {
  subtotalCent: number;
  shippingCent: number;
  taxCent: number;
  discountCent: number;
  totalCent: number;
  currency: string;
  shippingMethods: Array<{
    shippingMethodId: string;
    name: string;
    description: string;
    amountCent: number;
  }>;
};

export type OrderPreview = {
  orderId: number;
  orderNo: string;
  status: string;
  amountCent: number;
  currency: string;
  itemCount: number;
  createTime: string;
};
