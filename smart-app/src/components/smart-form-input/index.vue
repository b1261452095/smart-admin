<template>
  <view class="input-group">
    <view class="input-label" v-if="label || icon">
      <image v-if="icon" :src="icon" mode="aspectFit"></image>
      <text v-if="label">{{ label }}</text>
    </view>
    <view class="input-wrapper" :class="{ 'has-suffix': $slots.suffix }">
      <uni-easyinput
        class="custom-input"
        :class="inputClass"
        :placeholder="placeholder"
        :clearable="clearable"
        :password="password"
        :placeholderStyle="placeholderStyle"
        border="none"
        :value="modelValue"
        @input="handleInput"
        @blur="handleBlur"
      />
      <slot name="suffix"></slot>
    </view>
    <text v-if="error" class="error-text">{{ error }}</text>
    <slot name="extra"></slot>
  </view>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  modelValue: {
    type: [String, Number],
    default: '',
  },
  label: {
    type: String,
    default: '',
  },
  icon: {
    type: String,
    default: '',
  },
  placeholder: {
    type: String,
    default: '',
  },
  clearable: {
    type: Boolean,
    default: true,
  },
  password: {
    type: Boolean,
    default: false,
  },
  placeholderStyle: {
    type: String,
    default: 'color:#999999',
  },
  error: {
    type: String,
    default: '',
  },
  inputClass: {
    type: String,
    default: '',
  },
});

const emit = defineEmits(['update:modelValue', 'input', 'blur']);

function handleInput(value) {
  emit('update:modelValue', value);
  emit('input', value);
}

function handleBlur() {
  emit('blur');
}
</script>

<style lang="scss" scoped>
.input-group {
  margin-bottom: 30rpx;
  
  .input-label {
    display: flex;
    align-items: center;
    margin-bottom: 16rpx;
    
    image {
      width: 36rpx;
      height: 36rpx;
      margin-right: 16rpx;
    }
    
    text {
      font-size: 28rpx;
      color: #333333;
      font-weight: 500;
    }
  }
  
  .input-wrapper {
    position: relative;
    width: 100%;
    
    &.has-suffix {
      display: flex;
      align-items: center;
      gap: 20rpx;
    }
    
    .custom-input {
      background: #f7f8fa;
      display: flex;
      align-items: center;
      border-radius: 16rpx;
      padding: 0 24rpx;
      height: 88rpx;
      line-height: 88rpx;
      width: 100%;
      box-sizing: border-box;
      flex: 1;
      
      &.password-input {
        padding-right: 80rpx !important;
      }
    }
  }
  
  .error-text {
    display: block;
    font-size: 22rpx;
    color: #ff4d4f;
    margin-top: 8rpx;
    margin-left: 8rpx;
  }
}
</style>

