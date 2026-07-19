/*
 * Shop category.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopCategoryApi = {
  queryTree: (param) => {
    return postRequest('/shop/category/tree', param);
  },

  get: (categoryId) => {
    return getRequest(`/shop/category/get/${categoryId}`);
  },

  add: (param) => {
    return postRequest('/shop/category/add', param);
  },

  update: (param) => {
    return postRequest('/shop/category/update', param);
  },

  updateDisabled: (param) => {
    return postRequest('/shop/category/updateDisabled', param);
  },

  delete: (categoryId) => {
    return getRequest(`/shop/category/delete/${categoryId}`);
  },
};
