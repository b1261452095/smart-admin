/*
 * Shop product.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopProductApi = {
  queryPage: (param) => {
    return postRequest('/shop/product/queryPage', param);
  },

  get: (productId) => {
    return getRequest(`/shop/product/get/${productId}`);
  },

  add: (param) => {
    return postRequest('/shop/product/add', param);
  },

  update: (param) => {
    return postRequest('/shop/product/update', param);
  },

  updateShelves: (param) => {
    return postRequest('/shop/product/updateShelves', param);
  },

  delete: (productId) => {
    return getRequest(`/shop/product/delete/${productId}`);
  },
};
