import { reactive, ref, computed } from 'vue';

/**
 * 表单验证 Hook - 简化版本
 * @param {Array<string>} fields - 字段名数组
 * @returns {Object} 返回表单验证相关的方法和状态
 */
export function useFormValidator(fields = []) {
  // 错误信息
  const errors = reactive(
    fields.reduce((acc, key) => {
      acc[key] = '';
      return acc;
    }, {})
  );

  // 记录每个字段是否被用户操作过（touched）
  const touched = reactive(
    fields.reduce((acc, key) => {
      acc[key] = false;
      return acc;
    }, {})
  );
  
  // 是否已经尝试提交过表单
  const formSubmitted = ref(false);

  /**
   * 标记字段已被操作
   * @param {string} field - 字段名
   */
  function markTouched(field) {
    if (touched.hasOwnProperty(field)) {
      touched[field] = true;
    }
  }

  /**
   * 标记所有字段已被操作
   */
  function markAllTouched() {
    Object.keys(touched).forEach((key) => {
      touched[key] = true;
    });
  }

  /**
   * 判断是否应该显示错误信息
   * @param {string} field - 字段名
   * @returns {boolean}
   */
  function shouldShowError(field) {
    return !!(touched[field] || formSubmitted.value);
  }

  /**
   * 设置字段错误
   * @param {string} field - 字段名
   * @param {string} message - 错误信息
   */
  function setError(field, message) {
    if (errors.hasOwnProperty(field)) {
      errors[field] = message;
    }
  }

  /**
   * 清除某个字段的错误
   * @param {string} field - 字段名
   */
  function clearError(field) {
    if (errors.hasOwnProperty(field)) {
      errors[field] = '';
    }
  }

  /**
   * 清除所有错误
   */
  function clearAllErrors() {
    Object.keys(errors).forEach((key) => {
      errors[key] = '';
    });
  }

  /**
   * 验证所有字段（提交时使用）
   * @param {Array<Function>} validators - 验证器函数数组
   * @returns {boolean} 所有字段是否都通过验证
   */
  function validateForm(validators = []) {
    formSubmitted.value = true;
    markAllTouched();
    
    // 执行所有验证器
    validators.forEach(validator => validator());
    
    // 检查是否有错误
    return Object.values(errors).every(error => !error);
  }

  /**
   * 重置表单验证状态
   */
  function resetValidation() {
    Object.keys(errors).forEach((key) => {
      errors[key] = '';
      touched[key] = false;
    });
    formSubmitted.value = false;
  }

  // 是否有错误
  const hasErrors = computed(() => {
    return Object.values(errors).some((error) => !!error);
  });

  return {
    errors,
    touched,
    formSubmitted,
    hasErrors,
    markTouched,
    markAllTouched,
    shouldShowError,
    setError,
    clearError,
    clearAllErrors,
    validateForm,
    resetValidation,
  };
}

/**
 * 常用验证规则
 */
export const validators = {
  /**
   * 必填验证
   * @param {string} message - 错误提示信息
   */
  required: (message = '此字段为必填项') => (value) => {
    return !value || value.trim() === '' ? message : '';
  },

  /**
   * 邮箱验证
   * @param {string} message - 错误提示信息
   */
  email: (message = '请输入有效的邮箱地址') => (value) => {
    if (!value) return '';
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return !emailRegex.test(value) ? message : '';
  },

  /**
   * 最小长度验证
   * @param {number} min - 最小长度
   * @param {string} message - 错误提示信息
   */
  minLength: (min, message) => (value) => {
    if (!value) return '';
    const msg = message || `长度不能少于${min}位`;
    return value.length < min ? msg : '';
  },

  /**
   * 最大长度验证
   * @param {number} max - 最大长度
   * @param {string} message - 错误提示信息
   */
  maxLength: (max, message) => (value) => {
    if (!value) return '';
    const msg = message || `长度不能超过${max}位`;
    return value.length > max ? msg : '';
  },

  /**
   * 长度范围验证
   * @param {number} min - 最小长度
   * @param {number} max - 最大长度
   * @param {string} message - 错误提示信息
   */
  lengthRange: (min, max, message) => (value) => {
    if (!value) return '';
    const msg = message || `长度必须在${min}-${max}位之间`;
    return value.length < min || value.length > max ? msg : '';
  },

  /**
   * 密码强度验证（至少包含字母和数字）
   * @param {string} message - 错误提示信息
   */
  password: (message = '密码必须包含字母和数字') => (value) => {
    if (!value) return '';
    const hasLetter = /[a-zA-Z]/.test(value);
    const hasNumber = /[0-9]/.test(value);
    return !(hasLetter && hasNumber) ? message : '';
  },

  /**
   * 确认密码验证
   * @param {Function} getPassword - 获取原密码的函数
   * @param {string} message - 错误提示信息
   */
  confirmPassword: (getPassword, message = '两次密码输入不一致') => (value) => {
    if (!value) return '';
    return value !== getPassword() ? message : '';
  },

  /**
   * 手机号验证
   * @param {string} message - 错误提示信息
   */
  phone: (message = '请输入有效的手机号') => (value) => {
    if (!value) return '';
    const phoneRegex = /^1[3-9]\d{9}$/;
    return !phoneRegex.test(value) ? message : '';
  },

  /**
   * 自定义正则验证
   * @param {RegExp} regex - 正则表达式
   * @param {string} message - 错误提示信息
   */
  pattern: (regex, message = '格式不正确') => (value) => {
    if (!value) return '';
    return !regex.test(value) ? message : '';
  },
};