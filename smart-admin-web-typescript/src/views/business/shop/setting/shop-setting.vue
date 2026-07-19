<template>
  <a-card size="small" :bordered="false" :hoverable="true">
    <a-form ref="formRef" :model="form" :rules="rules" :label-col="{ span: 4 }" :wrapper-col="{ span: 12 }">
      <a-form-item label="店铺名称" name="storeName">
        <a-input v-model:value="form.storeName" placeholder="请输入店铺名称" />
      </a-form-item>

      <a-form-item label="店铺Logo" name="storeLogo">
        <Upload
          accept=".jpg,.jpeg,.png,.gif"
          :maxUploadSize="1"
          :maxSize="5"
          buttonText="上传Logo"
          :default-file-list="form.storeLogo"
          @change="storeLogoChange"
        />
      </a-form-item>

      <a-form-item label="店铺域名" name="storeDomain">
        <a-input v-model:value="form.storeDomain" placeholder="例如：https://shop.example.com" />
      </a-form-item>

      <a-form-item label="默认语言" name="defaultLanguage">
        <a-select v-model:value="form.defaultLanguage" placeholder="请选择默认语言">
          <a-select-option value="zh-CN">简体中文</a-select-option>
          <a-select-option value="en-US">English</a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="默认币种" name="defaultCurrency">
        <a-select v-model:value="form.defaultCurrency" placeholder="请选择默认币种">
          <a-select-option value="USD">USD</a-select-option>
          <a-select-option value="CNY">CNY</a-select-option>
          <a-select-option value="EUR">EUR</a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="客服邮箱" name="supportEmail">
        <a-input v-model:value="form.supportEmail" placeholder="请输入客服邮箱" />
      </a-form-item>

      <a-form-item label="交易开关">
        <a-space size="large">
          <a-checkbox v-model:checked="form.checkoutEnabledFlag">允许结账</a-checkbox>
          <a-checkbox v-model:checked="form.taxEnabledFlag">启用税费</a-checkbox>
          <a-checkbox v-model:checked="form.maintenanceEnabledFlag">维护模式</a-checkbox>
        </a-space>
      </a-form-item>

      <a-form-item label="SEO标题" name="seoTitle">
        <a-input v-model:value="form.seoTitle" placeholder="请输入SEO标题" />
      </a-form-item>

      <a-form-item label="SEO描述" name="seoDescription">
        <a-textarea v-model:value="form.seoDescription" :rows="3" placeholder="请输入SEO描述" />
      </a-form-item>

      <a-form-item label="备注" name="remark">
        <a-textarea v-model:value="form.remark" :rows="3" placeholder="内部备注，不展示给用户" />
      </a-form-item>

      <a-form-item :wrapper-col="{ offset: 4, span: 12 }">
        <a-space>
          <a-button type="primary" @click="submit" v-privilege="'shop:setting:update'">
            <template #icon>
              <SaveOutlined />
            </template>
            保存
          </a-button>
          <a-button @click="loadSetting" v-privilege="'shop:setting:query'">
            <template #icon>
              <ReloadOutlined />
            </template>
            重新加载
          </a-button>
        </a-space>
      </a-form-item>
    </a-form>
  </a-card>
</template>

<script setup lang="ts">
  import { onMounted, reactive, ref } from 'vue';
  import { message } from 'ant-design-vue';
  import Upload from '/@/components/support/file-upload/index.vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { shopSettingApi } from '/@/api/business/shop/shop-setting-api';

  const formRef = ref();

  const formDefault = {
    settingId: undefined,
    tenantId: 1,
    storeName: 'Smart Shop',
    storeLogo: [],
    storeDomain: '',
    defaultLanguage: 'zh-CN',
    defaultCurrency: 'USD',
    supportEmail: '',
    taxEnabledFlag: false,
    checkoutEnabledFlag: true,
    maintenanceEnabledFlag: false,
    seoTitle: '',
    seoDescription: '',
    remark: '',
  };

  const form = reactive<any>({ ...formDefault });

  const rules = {
    storeName: [{ required: true, message: '请输入店铺名称' }],
    defaultLanguage: [{ required: true, message: '请选择默认语言' }],
    defaultCurrency: [{ required: true, message: '请选择默认币种' }],
  };

  onMounted(() => {
    loadSetting();
  });

  async function loadSetting() {
    SmartLoading.show();
    try {
      const result = await shopSettingApi.get();
      Object.assign(form, formDefault, result.data || {});
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function submit() {
    formRef.value
      .validate()
      .then(async () => {
        SmartLoading.show();
        try {
          await shopSettingApi.update(form);
          message.success('保存成功');
          await loadSetting();
        } catch (error) {
          smartSentry.captureError(error);
        } finally {
          SmartLoading.hide();
        }
      })
      .catch(() => {
        message.error('参数验证错误，请检查表单');
      });
  }

  function storeLogoChange(fileList) {
    form.storeLogo = fileList;
  }
</script>
