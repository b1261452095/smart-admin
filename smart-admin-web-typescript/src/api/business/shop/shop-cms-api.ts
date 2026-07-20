/*
 * Shop CMS block.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopCmsApi = {
  queryPage: (param) => {
    return postRequest('/shop/cms/block/queryPage', param);
  },

  get: (blockId) => {
    return getRequest(`/shop/cms/block/get/${blockId}`);
  },

  add: (param) => {
    return postRequest('/shop/cms/block/add', param);
  },

  update: (param) => {
    return postRequest('/shop/cms/block/update', param);
  },

  updateDisabled: (param) => {
    return postRequest('/shop/cms/block/updateDisabled', param);
  },

  delete: (blockId) => {
    return getRequest(`/shop/cms/block/delete/${blockId}`);
  },

  clientList: (param) => {
    return postRequest('/shop/client/cms/block/list', param);
  },
};
