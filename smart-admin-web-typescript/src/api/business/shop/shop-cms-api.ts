/*
 * Shop CMS block.
 */
import { getRequest, postRequest } from '/@/lib/axios';

type CmsRequest = Record<string, unknown>;

export const shopCmsApi = {
  queryPage: (param: CmsRequest) => {
    return postRequest('/shop/cms/block/queryPage', param);
  },

  queryList: (param: CmsRequest) => {
    return postRequest('/shop/cms/block/queryList', param);
  },

  get: (blockId: number) => {
    return getRequest(`/shop/cms/block/get/${blockId}`, {});
  },

  add: (param: CmsRequest) => {
    return postRequest('/shop/cms/block/add', param);
  },

  update: (param: CmsRequest) => {
    return postRequest('/shop/cms/block/update', param);
  },

  updateDisabled: (param: CmsRequest) => {
    return postRequest('/shop/cms/block/updateDisabled', param);
  },

  updateSort: (param: CmsRequest) => {
    return postRequest('/shop/cms/block/updateSort', param);
  },

  delete: (blockId: number) => {
    return getRequest(`/shop/cms/block/delete/${blockId}`, {});
  },

  clientList: (param: CmsRequest) => {
    return postRequest('/shop/client/cms/block/list', param);
  },
};
