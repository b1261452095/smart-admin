import { ref } from 'vue';
import { loginApi } from '@/api/system/login-api';
import { smartSentry } from '@/lib/smart-sentry';

/**
 * 验证码管理 Hook
 * @returns {Object} 验证码相关的方法和状态
 */
export function useCaptcha() {
  const captchaBase64Image = ref('');
  const captchaUuid = ref('');
  const captchaCountdown = ref(0);
  
  let captchaCountdownTimer = null;
  let refreshCaptchaInterval = null;

  /**
   * 获取验证码
   */
  async function getCaptcha() {
    try {
      const captchaResult = await loginApi.getCaptcha();
      captchaBase64Image.value = captchaResult.data.captchaBase64Image;
      captchaUuid.value = captchaResult.data.captchaUuid;
      
      // 开始倒计时
      startCaptchaCountdown(captchaResult.data.expireSeconds);
      
      // 设置自动刷新
      beginRefreshCaptchaInterval(captchaResult.data.expireSeconds);
      
      return captchaResult.data;
    } catch (e) {
      smartSentry.captureError(e);
      throw e;
    }
  }

  /**
   * 开始验证码倒计时
   */
  function startCaptchaCountdown(expireSeconds) {
    // 清除之前的倒计时
    if (captchaCountdownTimer) {
      clearInterval(captchaCountdownTimer);
    }
    
    captchaCountdown.value = expireSeconds;
    captchaCountdownTimer = setInterval(() => {
      if (captchaCountdown.value > 0) {
        captchaCountdown.value--;
      } else {
        clearInterval(captchaCountdownTimer);
        captchaCountdownTimer = null;
      }
    }, 1000);
  }

  /**
   * 开始自动刷新验证码
   */
  function beginRefreshCaptchaInterval(expireSeconds) {
    if (refreshCaptchaInterval === null) {
      refreshCaptchaInterval = setInterval(getCaptcha, (expireSeconds - 5) * 1000);
    }
  }

  /**
   * 停止验证码刷新和倒计时
   */
  function stopRefreshCaptchaInterval() {
    if (refreshCaptchaInterval != null) {
      clearInterval(refreshCaptchaInterval);
      refreshCaptchaInterval = null;
    }
    if (captchaCountdownTimer) {
      clearInterval(captchaCountdownTimer);
      captchaCountdown.value = 0;
      captchaCountdownTimer = null;
    }
  }

  return {
    captchaBase64Image,
    captchaUuid,
    captchaCountdown,
    getCaptcha,
    stopRefreshCaptchaInterval,
  };
}