/*
 * Shop setting.
 */
import { getRequest, postRequest } from '/@/lib/axios';

export const shopSettingApi = {
  get: () => {
    return getRequest('/shop/setting/get');
  },

  update: (param) => {
    return postRequest('/shop/setting/update', param);
  },
};
