<template>
  <view class="container">
    <!-- 背景装饰 -->
    <view class="bg-decoration">
      <view class="circle circle1"></view>
      <view class="circle circle2"></view>
      <view class="circle circle3"></view>
    </view>

    <!-- 顶部区域 -->
    <view class="top-section">
      <view class="back-btn" @click="goBack">
        <image src="@/static/common/back-icon.png" mode="aspectFit"></image>
      </view>
      <view class="title-box">
        <text class="main-title">创建账号</text>
        <text class="sub-title">Create Your Account</text>
      </view>
    </view>

    <!-- 表单区域 -->
    <view class="form-section">
      <view class="form-card">
        <!-- 用户名 -->
        <SmartFormInput
          label="用户名"
          icon="@/static/images/login/login-username.png"
          placeholder="请输入用户名"
          v-model="registerForm.loginName"
          :error="errors.loginName"
          @input="handleFieldInput('loginName')"
          @blur="handleFieldBlur('loginName')"
        />

        <!-- 邮箱 -->
        <SmartFormInput
          label="邮箱"
          icon="@/static/images/login/login-username.png"
          placeholder="请输入邮箱地址"
          v-model="registerForm.email"
          :error="errors.email"
          @input="handleFieldInput('email')"
          @blur="handleFieldBlur('email')"
        />

        <!-- 密码 -->
        <SmartFormInput
          label="密码"
          icon="@/static/images/login/login-password.png"
          placeholder="请输入密码(6-20位)"
          v-model="registerForm.password"
          :password="!isPasswordVisible"
          :error="errors.password"
          input-class="password-input"
          @input="handlePasswordInput"
          @blur="handleFieldBlur('password')"
        >
          <template #suffix>
            <image 
              class="password-toggle-btn" 
              :src="isPasswordVisible ? '@/static/images/login/eye-open.png' : '@/static/images/login/eye-close.png'"
              mode="aspectFit"
              @click="togglePasswordVisibility"
            ></image>
          </template>
          <template #extra>
            <!-- 密码强度提示 -->
            <view v-if="registerForm.password" class="password-strength">
              <text class="strength-label">密码强度：</text>
              <text class="strength-text" :style="{ color: passwordStrength.color }">
                {{ passwordStrength.text }}
              </text>
              <view class="strength-bar">
                <view 
                  class="strength-fill" 
                  :style="{ 
                    width: `${passwordStrength.level * 25}%`,
                    backgroundColor: passwordStrength.color 
                  }"
                ></view>
              </view>
            </view>
          </template>
        </SmartFormInput>

        <!-- 确认密码 -->
        <SmartFormInput
          label="确认密码"
          icon="@/static/images/login/login-password.png"
          placeholder="请再次输入密码"
          v-model="registerForm.confirmPassword"
          :password="!isConfirmPasswordVisible"
          :error="errors.confirmPassword"
          input-class="password-input"
          @input="handleFieldInput('confirmPassword')"
          @blur="handleFieldBlur('confirmPassword')"
        >
          <template #suffix>
            <image 
              class="password-toggle-btn" 
              :src="isConfirmPasswordVisible ? '@/static/images/login/eye-open.png' : '@/static/images/login/eye-close.png'"
              mode="aspectFit"
              @click="toggleConfirmPasswordVisibility"
            ></image>
          </template>
        </SmartFormInput>

        <!-- 验证码 -->
        <SmartFormInput
          label="验证码"
          icon="@/static/images/login/login-password.png"
          placeholder="请输入验证码"
          v-model="registerForm.captchaCode"
          :error="errors.captchaCode"
          input-class="captcha-input"
          @input="handleFieldInput('captchaCode')"
          @blur="handleFieldBlur('captchaCode')"
        >
          <template #suffix>
            <image 
              class="captcha-img" 
              :src="captchaBase64Image" 
              @click="handleGetCaptcha" 
              mode="aspectFit"
            ></image>
          </template>
        </SmartFormInput>

        <!-- 注册按钮 -->
        <view class="btn-group">
          <button 
            class="register-btn" 
            @click="handleRegister"
            :disabled="!loginCheckBoxRef?.agreeFlag"
            :class="{ 'disabled': !loginCheckBoxRef?.agreeFlag }"
          >
            <text>立即注册</text>
          </button>
        </view>

        <!-- 底部提示 -->
        <view class="bottom-tips">
          <text class="tips-text">已有账号？</text>
          <text class="link-text" @click="goLogin">立即登录</text>
        </view>
      </view>
    </view>

    <!-- 协议 -->
    <LoginCheckBox class="check-box" ref="loginCheckBoxRef" />
  </view>
</template>

<script setup>
import { reactive, ref } from 'vue';
import { onShow } from '@dcloudio/uni-app';
import LoginCheckBox from '../login/components/login-check-box.vue';
import SmartFormInput from '@/components/smart-form-input/index.vue';
import { loginApi } from '@/api/system/login-api';
import { encryptData } from '@/lib/encrypt';
import { smartSentry } from '@/lib/smart-sentry';
import { useFormValidator } from '@/hooks/useFormValidator';
import { useCaptcha } from '@/hooks/useCaptcha';
import { usePasswordVisibility, usePasswordStrength } from '@/hooks/usePassword';

// 表单数据
const registerForm = reactive({
  loginName: '',
  email: '',
  password: '',
  confirmPassword: '',
  captchaCode: '',
  captchaUuid: '',
});

// 表单验证
const { 
  errors, 
  markTouched, 
  shouldShowError, 
  setError, 
  validateForm 
} = useFormValidator([
  'loginName',
  'email',
  'password',
  'confirmPassword',
  'captchaCode',
]);

// 验证码管理
const { 
  captchaBase64Image, 
  captchaUuid, 
  getCaptcha 
} = useCaptcha();

// 密码可见性
const { 
  isPasswordVisible, 
  togglePasswordVisibility 
} = usePasswordVisibility();

// 确认密码可见性
const { 
  isPasswordVisible: isConfirmPasswordVisible, 
  togglePasswordVisibility: toggleConfirmPasswordVisibility 
} = usePasswordVisibility();

// 密码强度
const { 
  passwordStrength, 
  calculatePasswordStrength 
} = usePasswordStrength();

const loginCheckBoxRef = ref();

// 同步 captchaUuid 到表单
const syncCaptchaUuid = () => {
  registerForm.captchaUuid = captchaUuid.value;
};

// 字段验证规则
function validateField(field) {
  const showError = shouldShowError(field);

  switch (field) {
    case 'loginName':
      setError('loginName', showError && !registerForm.loginName ? '请输入用户名' : '');
      break;
      
    case 'email':
      if (!registerForm.email) {
        setError('email', showError ? '请输入邮箱地址' : '');
      } else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(registerForm.email)) {
        setError('email', showError ? '请输入有效的邮箱地址' : '');
      } else {
        setError('email', '');
      }
      break;
      
    case 'password':
      if (!registerForm.password) {
        setError('password', showError ? '请输入密码' : '');
      } else if (registerForm.password.length < 6) {
        setError('password', showError ? '密码长度不能少于6位' : '');
      } else if (registerForm.password.length > 20) {
        setError('password', showError ? '密码长度不能超过20位' : '');
      } else {
        setError('password', '');
      }
      break;
      
    case 'confirmPassword':
      if (!registerForm.confirmPassword) {
        setError('confirmPassword', showError ? '请再次输入密码' : '');
      } else if (registerForm.password && registerForm.password !== registerForm.confirmPassword) {
        setError('confirmPassword', showError ? '两次密码输入不一致' : '');
      } else {
        setError('confirmPassword', '');
      }
      break;
      
    case 'captchaCode':
      setError('captchaCode', showError && !registerForm.captchaCode ? '请输入验证码' : '');
      break;
  }
}

// 处理字段输入
function handleFieldInput(field) {
  markTouched(field);
}

// 处理字段失焦
function handleFieldBlur(field) {
  markTouched(field);
  validateField(field);
}

// 处理密码输入（包含强度计算）
function handlePasswordInput() {
  markTouched('password');
  calculatePasswordStrength(registerForm.password);
  validateField('password');
  // 如果确认密码已输入，也需要重新验证
  if (registerForm.confirmPassword) {
    validateField('confirmPassword');
  }
}

// 验证所有字段
function validateAllFields() {
  const validators = [
    () => validateField('loginName'),
    () => validateField('email'),
    () => validateField('password'),
    () => validateField('confirmPassword'),
    () => validateField('captchaCode'),
  ];

  return validateForm(validators);
}

// 获取验证码
async function handleGetCaptcha() {
  await getCaptcha();
  syncCaptchaUuid();
}

// 注册
async function handleRegister() {
  if (!loginCheckBoxRef.value?.agreeFlag) {
    uni.showToast({
      icon: 'none',
      title: '请阅读并同意《用户协议》、《隐私政策》',
    });
    return;
  }

  if (!validateAllFields()) {
    return;
  }

  try {
    uni.showLoading({ title: '注册中...' });
    
    // 密码加密
    const encryptRegisterForm = Object.assign({}, registerForm, {
      password: encryptData(registerForm.password),
      confirmPassword: encryptData(registerForm.confirmPassword)
    });
    
    await loginApi.register(encryptRegisterForm);
    uni.hideLoading();
    uni.showToast({ title: '注册成功' });
    
    setTimeout(() => {
      goLogin();
    }, 1500);
  } catch (e) {
    uni.hideLoading();
    await handleGetCaptcha();
    smartSentry.captureError(e);
  }
}

// 返回
function goBack() {
  uni.navigateBack();
}

// 跳转登录
function goLogin() {
  uni.navigateBack();
}

// 页面显示时初始化
onShow(async () => {
  await handleGetCaptcha();
});
</script>

<style lang="scss" scoped>
.container {
  position: relative;
  min-height: 100vh;
  width: 100%;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  overflow-x: hidden;
  overflow-y: auto;
  box-sizing: border-box;
}

.bg-decoration {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  overflow: hidden;
  
  .circle {
    position: absolute;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.1);
    
    &.circle1 {
      width: 400rpx;
      height: 400rpx;
      top: -100rpx;
      right: -100rpx;
    }
    
    &.circle2 {
      width: 300rpx;
      height: 300rpx;
      bottom: 100rpx;
      left: -80rpx;
    }
    
    &.circle3 {
      width: 200rpx;
      height: 200rpx;
      top: 50%;
      right: 50rpx;
    }
  }
}

.top-section {
  position: relative;
  z-index: 10;
  padding: 60rpx 40rpx 40rpx;
  box-sizing: border-box;
  
  .back-btn {
    width: 80rpx;
    height: 80rpx;
    background: rgba(255, 255, 255, 0.2);
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    backdrop-filter: blur(10px);
    
    image {
      width: 40rpx;
      height: 40rpx;
    }
  }
  
  .title-box {
    margin-top: 40rpx;
    
    .main-title {
      display: block;
      font-size: 52rpx;
      font-weight: bold;
      color: #ffffff;
      margin-bottom: 16rpx;
    }
    
    .sub-title {
      display: block;
      font-size: 28rpx;
      color: rgba(255, 255, 255, 0.8);
      letter-spacing: 2rpx;
    }
  }
}

.form-section {
  position: relative;
  z-index: 10;
  padding: 0 40rpx 40rpx;
  box-sizing: border-box;
  
  .form-card {
    background: #ffffff;
    border-radius: 40rpx;
    padding: 50rpx 30rpx 40rpx;
    box-shadow: 0 20rpx 60rpx rgba(0, 0, 0, 0.1);
    box-sizing: border-box;
    width: 100%;
    
    .password-input {
      padding-right: 80rpx !important;
    }
    
    .password-toggle-btn {
      width: 48rpx;
      height: 48rpx;
      cursor: pointer;
    }
    
    .captcha-img {
      flex-shrink: 0;
      width: 180rpx;
      height: 88rpx;
      border-radius: 16rpx;
      background: #f7f8fa;
    }
    
    .password-strength {
      margin-top: 12rpx;
      margin-left: 8rpx;
      
      .strength-label {
        font-size: 24rpx;
        color: #666666;
        margin-right: 12rpx;
      }
      
      .strength-text {
        font-size: 24rpx;
        font-weight: 500;
      }
      
      .strength-bar {
        margin-top: 12rpx;
        width: 100%;
        height: 6rpx;
        background-color: #f0f0f0;
        border-radius: 3rpx;
        overflow: hidden;
        
        .strength-fill {
          height: 100%;
          border-radius: 3rpx;
          transition: all 0.3s ease;
        }
      }
    }
    
    .btn-group {
      margin-top: 40rpx;
      
      .register-btn {
        width: 100%;
        height: 88rpx;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 44rpx;
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: 0 10rpx 30rpx rgba(102, 126, 234, 0.4);
        border: none;
        padding: 0;
        box-sizing: border-box;
        transition: all 0.3s ease;
        
        text {
          font-size: 32rpx;
          color: #ffffff;
          font-weight: 500;
        }
        
        &:active {
          opacity: 0.8;
        }
        
        &.disabled {
          background: #cccccc;
          box-shadow: none;
          opacity: 0.6;
          
          &:active {
            opacity: 0.6;
          }
        }
      }
    }
    
    .bottom-tips {
      margin-top: 30rpx;
      text-align: center;
      
      .tips-text {
        font-size: 28rpx;
        color: #999999;
      }
      
      .link-text {
        font-size: 28rpx;
        color: #667eea;
        font-weight: 500;
        margin-left: 10rpx;
      }
    }
  }
}

.check-box {
  position: relative;
  z-index: 10;
  margin: 30rpx 40rpx 40rpx;
  box-sizing: border-box;
}

::v-deep .uni-easyinput__content {
  background-color: transparent !important;
}

::v-deep .is-input-border {
  border: none;
}
</style>
