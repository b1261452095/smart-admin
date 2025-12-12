import { ref, computed } from 'vue';

/**
 * 密码可见性管理 Hook
 * @returns {Object} 密码可见性相关的方法和状态
 */
export function usePasswordVisibility() {
  const isPasswordVisible = ref(false);

  function togglePasswordVisibility() {
    isPasswordVisible.value = !isPasswordVisible.value;
  }

  return {
    isPasswordVisible,
    togglePasswordVisibility,
  };
}

/**
 * 密码强度计算 Hook
 * @returns {Object} 密码强度相关的方法和状态
 */
export function usePasswordStrength() {
  const passwordStrength = ref({
    level: 0, // 0-4，0：无，1：弱，2：中，3：强，4：极强
    text: '无',
    color: '#999999'
  });

  /**
   * 计算密码强度
   * @param {string} password - 密码
   */
  function calculatePasswordStrength(password) {
    let level = 0;
    let text = '无';
    let color = '#999999';
    
    if (!password) {
      passwordStrength.value = { level, text, color };
      return;
    }
    
    // 长度检查
    if (password.length >= 8) {
      level++;
    }
    
    // 包含小写字母
    if (/[a-z]/.test(password)) {
      level++;
    }
    
    // 包含大写字母
    if (/[A-Z]/.test(password)) {
      level++;
    }
    
    // 包含数字
    if (/[0-9]/.test(password)) {
      level++;
    }
    
    // 包含特殊字符
    if (/[^a-zA-Z0-9]/.test(password)) {
      level++;
    }
    
    // 限制最大强度为4
    level = Math.min(level, 4);
    
    // 根据强度设置文本和颜色
    switch (level) {
      case 0:
        text = '无';
        color = '#999999';
        break;
      case 1:
        text = '弱';
        color = '#ff4d4f';
        break;
      case 2:
        text = '中';
        color = '#faad14';
        break;
      case 3:
        text = '强';
        color = '#52c41a';
        break;
      case 4:
        text = '极强';
        color = '#1890ff';
        break;
    }
    
    passwordStrength.value = { level, text, color };
  }

  return {
    passwordStrength,
    calculatePasswordStrength,
  };
}