<template>
  <a-form class="smart-query-form" v-privilege="'shop:customer:query'">
    <a-row class="smart-query-form-row">
      <a-form-item label="关键字" class="smart-query-form-item">
        <a-input v-model:value="queryForm.searchWord" style="width: 260px" placeholder="客户编号/姓名/邮箱/手机号" />
      </a-form-item>

      <a-form-item label="状态" class="smart-query-form-item">
        <a-radio-group v-model:value="queryForm.disabledFlag" @change="onSearch">
          <a-radio-button :value="undefined">全部</a-radio-button>
          <a-radio-button :value="false">正常</a-radio-button>
          <a-radio-button :value="true">禁用</a-radio-button>
        </a-radio-group>
      </a-form-item>

      <a-form-item class="smart-query-form-item">
        <a-button-group>
          <a-button type="primary" @click="onSearch">
            <template #icon>
              <SearchOutlined />
            </template>
            查询
          </a-button>
          <a-button @click="resetQuery">
            <template #icon>
              <ReloadOutlined />
            </template>
            重置
          </a-button>
        </a-button-group>
      </a-form-item>
    </a-row>
  </a-form>

  <a-card size="small" :bordered="false">
    <a-table
      size="small"
      :dataSource="tableData"
      :columns="columns"
      rowKey="customerId"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1250 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'customerNo'">
          <div>{{ record.customerNo }}</div>
          <div class="smart-secondary-text">{{ registerSourceMap[record.registerSource] || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'customerName'">
          <div>{{ record.customerName || '-' }}</div>
          <div class="smart-secondary-text">{{ record.email || record.phone || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'disabledFlag'">
          <a-switch
            v-privilege="'shop:customer:update'"
            :checked="!record.disabledFlag"
            checked-children="正常"
            un-checked-children="禁用"
            @change="(checked) => updateDisabled(record, checked)"
          />
        </template>
        <template v-if="column.dataIndex === 'addressCount'">
          {{ record.addressCount || 0 }}
        </template>
        <template v-if="column.dataIndex === 'remark'">
          {{ record.remark || '-' }}
        </template>
        <template v-if="column.dataIndex === 'action'">
          <div class="smart-table-operate">
            <a-button type="link" @click="showDetail(record.customerId)" v-privilege="'shop:customer:detail'">详情</a-button>
            <a-button type="link" @click="showRemarkModal(record)" v-privilege="'shop:customer:remark'">备注</a-button>
          </div>
        </template>
      </template>
    </a-table>

    <div class="smart-query-table-page">
      <a-pagination
        showSizeChanger
        showQuickJumper
        show-less-items
        :pageSizeOptions="PAGE_SIZE_OPTIONS"
        :defaultPageSize="queryForm.pageSize"
        v-model:current="queryForm.pageNum"
        v-model:pageSize="queryForm.pageSize"
        :total="total"
        @change="queryPage"
        :show-total="showTableTotal"
      />
    </div>
  </a-card>

  <a-modal :open="detailVisible" title="客户详情" :width="980" :footer="null" forceRender @cancel="closeDetail">
    <a-descriptions size="small" bordered :column="2" class="smart-margin-bottom10">
      <a-descriptions-item label="客户编号">{{ detail.customerNo }}</a-descriptions-item>
      <a-descriptions-item label="客户名称">{{ detail.customerName || '-' }}</a-descriptions-item>
      <a-descriptions-item label="邮箱">{{ detail.email || '-' }}</a-descriptions-item>
      <a-descriptions-item label="手机号">{{ detail.phone || '-' }}</a-descriptions-item>
      <a-descriptions-item label="注册来源">{{ registerSourceMap[detail.registerSource] || '-' }}</a-descriptions-item>
      <a-descriptions-item label="状态">{{ detail.disabledFlag ? '禁用' : '正常' }}</a-descriptions-item>
      <a-descriptions-item label="最后登录">{{ detail.lastLoginTime || '-' }}</a-descriptions-item>
      <a-descriptions-item label="注册时间">{{ detail.createTime || '-' }}</a-descriptions-item>
      <a-descriptions-item label="备注" :span="2">{{ detail.remark || '-' }}</a-descriptions-item>
    </a-descriptions>

    <a-table
      size="small"
      :dataSource="detail.addressList || []"
      :columns="addressColumns"
      rowKey="addressId"
      bordered
      :pagination="false"
      :scroll="{ x: 1000 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'defaultFlag'">
          <a-tag :color="record.defaultFlag ? 'green' : 'default'">{{ record.defaultFlag ? '默认' : '普通' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'addressDetail'">
          {{ record.countryCode || '' }} {{ record.province || '' }} {{ record.city || '' }} {{ record.district || '' }}
          {{ record.addressDetail || '' }}
        </template>
      </template>
    </a-table>
  </a-modal>

  <a-modal
    :open="remarkVisible"
    title="客户备注"
    :width="560"
    forceRender
    ok-text="确认"
    cancel-text="取消"
    @ok="submitRemark"
    @cancel="closeRemarkModal"
  >
    <a-form ref="remarkFormRef" :model="remarkForm" :label-col="{ span: 4 }" :wrapper-col="{ span: 18 }">
      <a-form-item label="备注" name="remark">
        <a-textarea v-model:value="remarkForm.remark" :rows="4" placeholder="请输入客户备注" />
      </a-form-item>
    </a-form>
  </a-modal>
</template>

<script setup lang="ts">
  import { onMounted, reactive, ref } from 'vue';
  import { message } from 'ant-design-vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { PAGE_SIZE, PAGE_SIZE_OPTIONS, showTableTotal } from '/@/constants/common-const';
  import { shopCustomerApi } from '/@/api/business/shop/shop-customer-api';

  const registerSourceMap = {
    1: '邮箱注册',
    2: '手机注册',
    3: '后台导入',
  };

  const columns = [
    { title: '客户编号', dataIndex: 'customerNo', width: 210 },
    { title: '租户', dataIndex: 'tenantId', width: 90 },
    { title: '客户', dataIndex: 'customerName', width: 240 },
    { title: '邮箱', dataIndex: 'email', width: 210 },
    { title: '手机号', dataIndex: 'phone', width: 150 },
    { title: '地址数', dataIndex: 'addressCount', width: 90 },
    { title: '状态', dataIndex: 'disabledFlag', width: 100 },
    { title: '最后登录', dataIndex: 'lastLoginTime', width: 170 },
    { title: '注册时间', dataIndex: 'createTime', width: 170 },
    { title: '备注', dataIndex: 'remark', width: 220 },
    { title: '操作', dataIndex: 'action', width: 120, fixed: 'right' },
  ];

  const addressColumns = [
    { title: '类型', dataIndex: 'defaultFlag', width: 90 },
    { title: '收货人', dataIndex: 'receiverName', width: 120 },
    { title: '手机号', dataIndex: 'receiverPhone', width: 150 },
    { title: '地址', dataIndex: 'addressDetail', width: 420 },
    { title: '邮编', dataIndex: 'postalCode', width: 100 },
    { title: '创建时间', dataIndex: 'createTime', width: 170 },
  ];

  const queryFormDefault = {
    tenantId: undefined,
    searchWord: '',
    disabledFlag: undefined,
    pageNum: 1,
    pageSize: PAGE_SIZE,
    searchCount: true,
  };
  const queryForm = reactive<any>({ ...queryFormDefault });
  const tableLoading = ref(false);
  const tableData = ref([]);
  const total = ref(0);

  onMounted(queryPage);

  function onSearch() {
    queryForm.pageNum = 1;
    queryPage();
  }

  function resetQuery() {
    Object.assign(queryForm, queryFormDefault);
    queryPage();
  }

  async function queryPage() {
    tableLoading.value = true;
    try {
      const result = await shopCustomerApi.queryPage(queryForm);
      tableData.value = result.data.list || [];
      total.value = result.data.total || 0;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      tableLoading.value = false;
    }
  }

  async function updateDisabled(record, checked) {
    SmartLoading.show();
    try {
      await shopCustomerApi.updateDisabled({
        customerId: record.customerId,
        disabledFlag: !checked,
      });
      message.success(checked ? '客户已启用' : '客户已禁用');
      await queryPage();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  const detailVisible = ref(false);
  const detail = reactive<any>({
    addressList: [],
  });

  async function showDetail(customerId) {
    SmartLoading.show();
    try {
      const result = await shopCustomerApi.detail(customerId);
      Object.assign(detail, result.data || { addressList: [] });
      detailVisible.value = true;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function closeDetail() {
    detailVisible.value = false;
    Object.assign(detail, { addressList: [] });
  }

  const remarkFormRef = ref();
  const remarkVisible = ref(false);
  const remarkForm = reactive<any>({
    customerId: undefined,
    remark: '',
  });

  function showRemarkModal(record) {
    Object.assign(remarkForm, {
      customerId: record.customerId,
      remark: record.remark || '',
    });
    remarkVisible.value = true;
  }

  function closeRemarkModal() {
    remarkVisible.value = false;
    Object.assign(remarkForm, { customerId: undefined, remark: '' });
  }

  async function submitRemark() {
    SmartLoading.show();
    try {
      await shopCustomerApi.updateRemark(remarkForm);
      message.success('备注更新成功');
      closeRemarkModal();
      await queryPage();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }
</script>
