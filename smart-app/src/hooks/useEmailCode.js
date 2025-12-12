import { ref } from 'vue';
import { loginApi } from '@/api/system/login-api';
import { smartSentry } from '@/lib/smart-sentry';

/**
 * 邮箱验证码管理 Hook
 * @returns {Object} 邮箱验证码相关的方法和状态
 */
export function useEmailCode() {
  const emailCodeShowFlag = ref(false);
  const emailCodeTips = ref('获取邮箱验证码');
  const emailCodeButtonDisabled = ref(false);
  
  let countDownTimer = null;

  /**
   * 开始倒计时
   */
  function runCountDown() {
    emailCodeButtonDisabled.value = true;
    let countDown = 60;
    emailCodeTips.value = `${countDown}秒后重新获取`;
    
    countDownTimer = setInterval(() => {
      if (countDown > 1) {
        countDown--;
        emailCodeTips.value = `${countDown}秒后重新获取`;
      } else {
        clearInterval(countDownTimer);
        countDownTimer = null;
        emailCodeButtonDisabled.value = false;
        emailCodeTips.value = '获取验证码';
      }
    }, 1000);
  }

  /**
   * 获取双因子登录标识
   */
  async function getTwoFactorLoginFlag() {
    try {
      const result = await loginApi.getTwoFactorLoginFlag();
      emailCodeShowFlag.value = result.data;
      return result.data;
    } catch (e) {
      smartSentry.captureError(e);
      throw e;
    }
  }

  /**
   * 发送邮箱验证码
   * @param {string} loginName - 用户名
   */
  async function sendEmailCode(loginName) {
    if (!loginName) {
      uni.showToast({
        icon: 'none',
        title: '请先输入用户名'
      });
      return;
    }
    
    try {
      uni.showLoading();
      await loginApi.sendLoginEmailCode(loginName);
      uni.showToast({
        icon: 'success',
        title: '验证码发送成功!请登录邮箱查看验证码~'
      });
      runCountDown();
    } catch (e) {
      smartSentry.captureError(e);
      throw e;
    } finally {
      uni.hideLoading();
    }
  }

  /**
   * 清理定时器
   */
  function cleanup() {
    if (countDownTimer) {
      clearInterval(countDownTimer);
      countDownTimer = null;
    }
  }

  return {
    emailCodeShowFlag,
    emailCodeTips,
    emailCodeButtonDisabled,
    getTwoFactorLoginFlag,
    sendEmailCode,
    cleanup,
  };
}