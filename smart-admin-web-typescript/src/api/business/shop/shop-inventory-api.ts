/*
 * Shop inventory.
 */
import { postRequest } from '/@/lib/axios';

export const shopInventoryApi = {
  queryPage: (param) => {
    return postRequest('/shop/inventory/queryPage', param);
  },

  adjust: (param) => {
    return postRequest('/shop/inventory/adjust', param);
  },

  queryRecordPage: (param) => {
    return postRequest('/shop/inventory/record/queryPage', param);
  },
};
