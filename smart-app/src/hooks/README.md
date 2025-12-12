# Hooks 使用文档

本目录包含了项目中可复用的 Vue 3 Composition API Hooks，用于简化表单验证、验证码管理、密码处理等常见功能。

## 目录

- [useFormValidator](#useformvalidator) - 表单验证
- [useCaptcha](#usecaptcha) - 图形验证码管理
- [usePassword](#usepassword) - 密码可见性和强度
- [useEmailCode](#useemailcode) - 邮箱验证码管理

---

## useFormValidator

表单验证 Hook，提供统一的表单验证逻辑和错误处理。

### 基础用法

```javascript
import { useFormValidator } from '@/hooks/useFormValidator';

const { 
  errors, 
  markTouched, 
  shouldShowError, 
  setError, 
  validateForm 
} = useFormValidator(['username', 'password', 'email']);

// 标记字段已触碰
markTouched('username');

// 设置错误信息
setError('username', '用户名不能为空');

// 验证表单
const isValid = validateForm([
  () => validateField('username'),
  () => validateField('password'),
]);
```

### API

#### 返回值

- `errors` - 错误信息对象
- `touched` - 字段触碰状态对象
- `formSubmitted` - 表单是否已提交
- `hasErrors` - 是否有错误（计算属性）
- `markTouched(field)` - 标记字段已触碰
- `markAllTouched()` - 标记所有字段已触碰
- `shouldShowError(field)` - 判断是否应该显示错误
- `setError(field, message)` - 设置字段错误
- `clearError(field)` - 清除字段错误
- `clearAllErrors()` - 清除所有错误
- `validateForm(validators)` - 验证表单
- `resetValidation()` - 重置验证状态

### 内置验证器

```javascript
import { validators } from '@/hooks/useFormValidator';

// 必填验证
validators.required('请输入用户名')

// 邮箱验证
validators.email('请输入有效的邮箱地址')

// 长度验证
validators.minLength(6, '密码长度不能少于6位')
validators.maxLength(20, '密码长度不能超过20位')
validators.lengthRange(6, 20, '密码长度必须在6-20位之间')

// 密码强度验证
validators.password('密码必须包含字母和数字')

// 确认密码验证
validators.confirmPassword(() => form.password, '两次密码输入不一致')

// 手机号验证
validators.phone('请输入有效的手机号')

// 自定义正则验证
validators.pattern(/^[a-zA-Z0-9]+$/, '只能包含字母和数字')
```

---

## useCaptcha

图形验证码管理 Hook，处理验证码的获取、刷新和倒计时。

### 基础用法

```javascript
import { useCaptcha } from '@/hooks/useCaptcha';
import { onUnload } from '@dcloudio/uni-app';

const { 
  captchaBase64Image, 
  captchaUuid, 
  captchaCountdown, 
  getCaptcha, 
  stopRefreshCaptchaInterval 
} = useCaptcha();

// 获取验证码
await getCaptcha();

// 页面卸载时清理
onUnload(() => {
  stopRefreshCaptchaInterval();
});
```

### API

#### 返回值

- `captchaBase64Image` - 验证码图片 Base64
- `captchaUuid` - 验证码 UUID
- `captchaCountdown` - 验证码倒计时（秒）
- `getCaptcha()` - 获取验证码
- `stopRefreshCaptchaInterval()` - 停止自动刷新

### 特性

- 自动倒计时显示
- 自动刷新机制（过期前5秒）
- 错误处理和日志记录

---

## usePassword

密码相关功能 Hook，包括密码可见性切换和密码强度计算。

### usePasswordVisibility - 密码可见性

```javascript
import { usePasswordVisibility } from '@/hooks/usePassword';

const { isPasswordVisible, togglePasswordVisibility } = usePasswordVisibility();

// 切换密码可见性
togglePasswordVisibility();
```

### usePasswordStrength - 密码强度

```javascript
import { usePasswordStrength } from '@/hooks/usePassword';

const { passwordStrength, calculatePasswordStrength } = usePasswordStrength();

// 计算密码强度
calculatePasswordStrength('MyPassword123');

// passwordStrength.value 包含：
// - level: 0-4 (0:无, 1:弱, 2:中, 3:强, 4:极强)
// - text: '弱' | '中' | '强' | '极强'
// - color: '#ff4d4f' | '#faad14' | '#52c41a' | '#1890ff'
```

### 密码强度规则

- 长度 >= 8: +1
- 包含小写字母: +1
- 包含大写字母: +1
- 包含数字: +1
- 包含特殊字符: +1

---

## useEmailCode

邮箱验证码管理 Hook，处理邮箱验证码的发送和倒计时。

### 基础用法

```javascript
import { useEmailCode } from '@/hooks/useEmailCode';
import { onUnload } from '@dcloudio/uni-app';

const { 
  emailCodeShowFlag, 
  emailCodeTips, 
  emailCodeButtonDisabled, 
  getTwoFactorLoginFlag, 
  sendEmailCode,
  cleanup
} = useEmailCode();

// 获取双因子登录标识
await getTwoFactorLoginFlag();

// 发送邮箱验证码
await sendEmailCode('username');

// 页面卸载时清理
onUnload(() => {
  cleanup();
});
```

### API

#### 返回值

- `emailCodeShowFlag` - 是否显示邮箱验证码输入框
- `emailCodeTips` - 按钮提示文本
- `emailCodeButtonDisabled` - 按钮是否禁用
- `getTwoFactorLoginFlag()` - 获取双因子登录标识
- `sendEmailCode(loginName)` - 发送邮箱验证码
- `cleanup()` - 清理定时器

### 特性

- 60秒倒计时
- 自动禁用按钮
- 错误处理和提示

---

## 最佳实践

### 1. 组合使用多个 Hooks

```javascript
import { useFormValidator } from '@/hooks/useFormValidator';
import { useCaptcha } from '@/hooks/useCaptcha';
import { usePasswordVisibility } from '@/hooks/usePassword';

const { errors, validateForm } = useFormValidator(['username', 'password']);
const { captchaBase64Image, getCaptcha } = useCaptcha();
const { isPasswordVisible, togglePasswordVisibility } = usePasswordVisibility();
```

### 2. 页面卸载时清理资源

```javascript
import { onUnload } from '@dcloudio/uni-app';

onUnload(() => {
  stopRefreshCaptchaInterval();
  cleanup();
});
```

### 3. 统一的错误处理

所有 Hooks 都使用 `smartSentry.captureError(e)` 进行错误记录，确保错误被正确追踪。

---

## 更新日志

### v1.0.0 (2024-12-12)

- ✨ 新增 `useFormValidator` - 表单验证 Hook
- ✨ 新增 `useCaptcha` - 验证码管理 Hook
- ✨ 新增 `usePassword` - 密码功能 Hook
- ✨ 新增 `useEmailCode` - 邮箱验证码 Hook
- 📝 完善文档和示例代码