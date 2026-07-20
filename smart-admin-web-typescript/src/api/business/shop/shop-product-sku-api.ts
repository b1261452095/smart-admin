/*
 * Shop product SKU.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopProductSkuApi = {
  queryList: (param) => {
    return postRequest('/shop/product/sku/queryList', param);
  },

  save: (param) => {
    return postRequest('/shop/product/sku/save', param);
  },

  saveList: (param) => {
    return postRequest('/shop/product/sku/saveList', param);
  },

  updateDisabled: (param) => {
    return postRequest('/shop/product/sku/updateDisabled', param);
  },

  delete: (skuId) => {
    return getRequest(`/shop/product/sku/delete/${skuId}`);
  },
};
