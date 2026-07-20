/*
 * Shop order.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopOrderApi = {
  queryPage: (param) => {
    return postRequest('/shop/order/queryPage', param);
  },

  detail: (orderId) => {
    return getRequest(`/shop/order/detail/${orderId}`);
  },

  updateRemark: (param) => {
    return postRequest('/shop/order/updateRemark', param);
  },

  cancel: (param) => {
    return postRequest('/shop/order/cancel', param);
  },
};
