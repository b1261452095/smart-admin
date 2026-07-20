/*
 * Shop customer.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopCustomerApi = {
  queryPage: (param) => {
    return postRequest('/shop/customer/queryPage', param);
  },

  detail: (customerId) => {
    return getRequest(`/shop/customer/detail/${customerId}`);
  },

  updateDisabled: (param) => {
    return postRequest('/shop/customer/updateDisabled', param);
  },

  updateRemark: (param) => {
    return postRequest('/shop/customer/updateRemark', param);
  },

  register: (param) => {
    return postRequest('/shop/client/customer/register', param);
  },

  login: (param) => {
    return postRequest('/shop/client/customer/login', param);
  },
};
