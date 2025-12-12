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
      <view class="title-box">
        <text class="main-title">欢迎回来</text>
        <text class="sub-title">Welcome Back</text>
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
          v-model="loginForm.loginName"
          :error="errors.loginName"
          @input="handleFieldInput('loginName')"
          @blur="handleFieldBlur('loginName')"
        />

        <!-- 邮箱验证码 -->
        <SmartFormInput
          v-if="emailCodeShowFlag"
          label="邮箱验证码"
          icon="@/static/images/login/login-password.png"
          placeholder="请输入邮箱验证码"
          v-model="loginForm.emailCode"
          :error="errors.emailCode"
          input-class="code-input"
          @input="handleFieldInput('emailCode')"
          @blur="handleFieldBlur('emailCode')"
        >
          <template #suffix>
            <button 
              @click="handleSendEmailCode" 
              class="code-btn" 
              :disabled="emailCodeButtonDisabled"
            >
              {{ emailCodeTips }}
            </button>
          </template>
        </SmartFormInput>

        <!-- 密码 -->
        <SmartFormInput
          label="密码"
          icon="@/static/images/login/login-password.png"
          placeholder="请输入密码"
          v-model="loginForm.password"
          :password="!isPasswordVisible"
          :error="errors.password"
          input-class="password-input"
          @input="handleFieldInput('password')"
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
        </SmartFormInput>

        <!-- 验证码 -->
        <SmartFormInput
          label="验证码"
          icon="@/static/images/login/login-password.png"
          placeholder="请输入验证码"
          v-model="loginForm.captchaCode"
          :error="errors.captchaCode"
          input-class="captcha-input"
          @input="handleFieldInput('captchaCode')"
          @blur="handleFieldBlur('captchaCode')"
        >
          <template #suffix>
            <view class="captcha-with-countdown">
              <image 
                class="captcha-img" 
                :src="captchaBase64Image" 
                @click="getCaptcha" 
                mode="aspectFit"
              ></image>
              <view v-if="captchaCountdown > 0" class="captcha-countdown">
                <text>{{ captchaCountdown }}s</text>
              </view>
            </view>
          </template>
        </SmartFormInput>

        <!-- 功能链接 -->
        <view class="function-links">
          <text class="link-text">验证码登录</text>
          <text class="link-text">忘记密码？</text>
        </view>

        <!-- 登录按钮 -->
        <view class="btn-group">
          <button 
            class="login-btn" 
            @click="handleLogin"
            :disabled="!loginCheckBoxRef?.agreeFlag"
            :class="{ 'disabled': !loginCheckBoxRef?.agreeFlag }"
          >
            <text>登录</text>
          </button>
          <button class="register-btn" @click="goRegister">
            <text>创建账号</text>
          </button>
        </view>
        
        <!-- 其他登录方式 -->
        <OtherWayBox />
      </view>
    </view>

    <!-- 协议 -->
    <LoginCheckBox class="check-box" ref="loginCheckBoxRef" />
  </view>
</template>

<script setup>
import { reactive, ref } from 'vue';
import { onShow, onUnload } from '@dcloudio/uni-app';
import OtherWayBox from './components/other-way-box.vue';
import LoginCheckBox from './components/login-check-box.vue';
import SmartFormInput from '@/components/smart-form-input/index.vue';
import { loginApi } from '@/api/system/login-api';
import { LOGIN_DEVICE_ENUM } from '@/constants/system/login-device-const';
import { encryptData } from '@/lib/encrypt';
import { useUserStore } from '@/store/modules/system/user';
import { smartSentry } from '@/lib/smart-sentry';
import { useFormValidator } from '@/hooks/useFormValidator';
import { useCaptcha } from '@/hooks/useCaptcha';
import { usePasswordVisibility } from '@/hooks/usePassword';
import { useEmailCode } from '@/hooks/useEmailCode';

// 表单数据
const loginForm = reactive({
  loginName: '',
  password: '',
  captchaCode: '',
  captchaUuid: '',
  emailCode: '',
  loginDevice: LOGIN_DEVICE_ENUM.H5.value,
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
  'password',
  'captchaCode',
  'emailCode',
]);

// 验证码管理
const { 
  captchaBase64Image, 
  captchaUuid, 
  captchaCountdown, 
  getCaptcha, 
  stopRefreshCaptchaInterval 
} = useCaptcha();

// 密码可见性
const { isPasswordVisible, togglePasswordVisibility } = usePasswordVisibility();

// 邮箱验证码
const { 
  emailCodeShowFlag, 
  emailCodeTips, 
  emailCodeButtonDisabled, 
  getTwoFactorLoginFlag, 
  sendEmailCode,
  cleanup: cleanupEmailCode
} = useEmailCode();

const loginCheckBoxRef = ref();

// 同步 captchaUuid 到表单
const syncCaptchaUuid = () => {
  loginForm.captchaUuid = captchaUuid.value;
};

// 字段验证规则
function validateField(field) {
  const showError = shouldShowError(field);

  switch (field) {
    case 'loginName':
      setError('loginName', showError && !loginForm.loginName ? '请输入用户名' : '');
      break;
    case 'password':
      setError('password', showError && !loginForm.password ? '请输入密码' : '');
      break;
    case 'captchaCode':
      setError('captchaCode', showError && !loginForm.captchaCode ? '请输入验证码' : '');
      break;
    case 'emailCode':
      setError('emailCode', showError && emailCodeShowFlag.value && !loginForm.emailCode ? '请输入邮箱验证码' : '');
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

// 验证所有字段
function validateAllFields() {
  const validators = [
    () => validateField('loginName'),
    () => validateField('password'),
    () => validateField('captchaCode'),
  ];

  if (emailCodeShowFlag.value) {
    validators.push(() => validateField('emailCode'));
  }

  return validateForm(validators);
}

// 发送邮箱验证码
async function handleSendEmailCode() {
  try {
    await sendEmailCode(loginForm.loginName);
  } catch (e) {
    // 错误已在 hook 中处理
  }
}

// 登录
async function handleLogin() {
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
    uni.showLoading({ title: '登录中' });
    
    // 密码加密
    const encryptPasswordForm = Object.assign({}, loginForm, {
      password: encryptData(loginForm.password),
    });
    
    const res = await loginApi.login(encryptPasswordForm);
    stopRefreshCaptchaInterval();
    
    uni.showToast({ title: '登录成功' });
    
    // 更新用户信息到 pinia
    useUserStore().setUserLoginInfo(res.data);

    uni.switchTab({ url: '/pages/home/index' });
  } catch (e) {
    if (e.data && e.data.code !== 0) {
      loginForm.captchaCode = '';
      await getCaptcha();
      syncCaptchaUuid();
    }
    smartSentry.captureError(e);
    uni.hideLoading();
  }
}

// 跳转注册页面
function goRegister() {
  uni.navigateTo({
    url: '/pages/register/register',
  });
}

// 页面显示时初始化
onShow(async () => {
  await getCaptcha();
  syncCaptchaUuid();
  await getTwoFactorLoginFlag();
});

// 页面卸载时清理
onUnload(() => {
  stopRefreshCaptchaInterval();
  cleanupEmailCode();
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
      width: 500rpx;
      height: 500rpx;
      top: -150rpx;
      right: -150rpx;
    }
    
    &.circle2 {
      width: 350rpx;
      height: 350rpx;
      bottom: 50rpx;
      left: -100rpx;
    }
    
    &.circle3 {
      width: 250rpx;
      height: 250rpx;
      top: 40%;
      right: 30rpx;
    }
  }
}

.top-section {
  position: relative;
  z-index: 10;
  padding: 80rpx 40rpx 30rpx;
  box-sizing: border-box;
  
  .title-box {
    margin-bottom: 40rpx;
    
    .main-title {
      display: block;
      font-size: 60rpx;
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
    
    .code-btn {
      flex-shrink: 0;
      width: 180rpx;
      height: 88rpx;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      border-radius: 16rpx;
      font-size: 24rpx;
      color: #ffffff;
      border: none;
      padding: 0;
      
      &:disabled {
        opacity: 0.6;
      }
    }
    
    .captcha-with-countdown {
      position: relative;
      flex-shrink: 0;
    }
    
    .captcha-img {
      width: 180rpx;
      height: 88rpx;
      border-radius: 16rpx;
      background: #f7f8fa;
    }
    
    .captcha-countdown {
      position: absolute;
      bottom: 0;
      right: 0;
      background: rgba(0, 0, 0, 0.6);
      color: #ffffff;
      font-size: 20rpx;
      padding: 2rpx 8rpx;
      border-radius: 0 0 16rpx 0;
      
      text {
        color: #ffffff;
      }
    }
    
    .function-links {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 40rpx;
      
      .link-text {
        font-size: 26rpx;
        color: #667eea;
        font-weight: 500;
      }
    }
    
    .btn-group {
      display: flex;
      flex-direction: column;
      gap: 24rpx;
      
      .login-btn {
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
      
      .register-btn {
        width: 100%;
        height: 88rpx;
        background: #ffffff;
        border: 2rpx solid #667eea;
        border-radius: 44rpx;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 0;
        box-sizing: border-box;
        
        text {
          font-size: 32rpx;
          color: #667eea;
          font-weight: 500;
        }
        
        &:active {
          opacity: 0.8;
        }
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
