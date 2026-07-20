<template>
  <a-form class="smart-query-form" v-privilege="'shop:inventory:query'">
    <a-row class="smart-query-form-row">
      <a-form-item label="关键字" class="smart-query-form-item">
        <a-input v-model:value="queryForm.searchWord" style="width: 240px" placeholder="商品/SKU名称或编码" />
      </a-form-item>

      <a-form-item label="商品" class="smart-query-form-item">
        <a-select
          v-model:value="queryForm.productId"
          style="width: 260px"
          show-search
          allow-clear
          :filter-option="filterProductOption"
          placeholder="全部商品"
        >
          <a-select-option v-for="item in productOptions" :key="item.productId" :value="item.productId" :label="item.productName">
            {{ item.productName }}
          </a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="库存预警" class="smart-query-form-item">
        <a-radio-group v-model:value="queryForm.warningFlag" @change="onSearch">
          <a-radio-button :value="undefined">全部</a-radio-button>
          <a-radio-button :value="true">只看预警</a-radio-button>
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
      rowKey="inventoryId"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1350 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'productName'">
          <div>{{ record.productName }}</div>
          <div class="smart-secondary-text">{{ record.productCode || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'skuName'">
          <div>{{ record.skuName }}</div>
          <div class="smart-secondary-text">{{ record.specSummary || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'skuCode'">
          {{ record.skuCode || '-' }}
        </template>
        <template v-if="column.dataIndex === 'stockStatus'">
          <a-tag :color="record.warningFlag ? 'red' : 'green'">{{ record.warningFlag ? '预警' : '正常' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'action'">
          <div class="smart-table-operate">
            <a-button type="link" @click="showAdjustModal(record)" v-privilege="'shop:inventory:adjust'">调整</a-button>
            <a-button type="link" @click="showRecordModal(record)" v-privilege="'shop:inventory:record:query'">流水</a-button>
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

  <a-modal
    :open="adjustVisible"
    title="调整库存"
    :width="560"
    forceRender
    ok-text="确认"
    cancel-text="取消"
    @ok="submitAdjust"
    @cancel="closeAdjustModal"
  >
    <a-descriptions size="small" bordered :column="1" class="smart-margin-bottom10">
      <a-descriptions-item label="商品">{{ currentInventory.productName }}</a-descriptions-item>
      <a-descriptions-item label="SKU">{{ currentInventory.skuName }} / {{ currentInventory.specSummary || '-' }}</a-descriptions-item>
      <a-descriptions-item label="当前可售">{{ currentInventory.availableStock }}</a-descriptions-item>
      <a-descriptions-item label="锁定/已售">{{ currentInventory.lockedStock }} / {{ currentInventory.soldStock }}</a-descriptions-item>
    </a-descriptions>

    <a-form ref="adjustFormRef" :model="adjustForm" :rules="adjustRules" :label-col="{ span: 5 }" :wrapper-col="{ span: 17 }">
      <a-form-item label="调整数量" name="changeQuantity" extra="正数表示入库，负数表示出库">
        <a-input-number v-model:value="adjustForm.changeQuantity" style="width: 100%" />
      </a-form-item>
      <a-form-item label="预警库存" name="warningStock">
        <a-input-number v-model:value="adjustForm.warningStock" style="width: 100%" :min="0" />
      </a-form-item>
      <a-form-item label="备注" name="remark">
        <a-textarea v-model:value="adjustForm.remark" :rows="3" placeholder="请输入调整原因" />
      </a-form-item>
    </a-form>
  </a-modal>

  <a-modal
    :open="recordVisible"
    title="库存流水"
    :width="980"
    :footer="null"
    forceRender
    @cancel="closeRecordModal"
  >
    <a-table
      size="small"
      :dataSource="recordData"
      :columns="recordColumns"
      rowKey="recordId"
      bordered
      :pagination="false"
      :loading="recordLoading"
      :scroll="{ x: 1100 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'operationType'">
          <a-tag color="blue">{{ record.operationType === 1 ? '手动调整' : '系统变更' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'changeQuantity'">
          <span :style="{ color: record.changeQuantity >= 0 ? '#389e0d' : '#cf1322' }">
            {{ record.changeQuantity >= 0 ? '+' : '' }}{{ record.changeQuantity }}
          </span>
        </template>
      </template>
    </a-table>

    <div class="smart-query-table-page">
      <a-pagination
        showSizeChanger
        showQuickJumper
        show-less-items
        :pageSizeOptions="PAGE_SIZE_OPTIONS"
        v-model:current="recordQueryForm.pageNum"
        v-model:pageSize="recordQueryForm.pageSize"
        :total="recordTotal"
        @change="queryRecordPage"
        :show-total="showTableTotal"
      />
    </div>
  </a-modal>
</template>

<script setup lang="ts">
  import { onMounted, reactive, ref } from 'vue';
  import { message } from 'ant-design-vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { PAGE_SIZE, PAGE_SIZE_OPTIONS, showTableTotal } from '/@/constants/common-const';
  import { shopProductApi } from '/@/api/business/shop/shop-product-api';
  import { shopInventoryApi } from '/@/api/business/shop/shop-inventory-api';

  const columns = [
    { title: '商品', dataIndex: 'productName', width: 220 },
    { title: 'SKU', dataIndex: 'skuName', width: 240 },
    { title: 'SKU编码', dataIndex: 'skuCode', width: 150 },
    { title: '币种', dataIndex: 'currency', width: 80 },
    { title: '可售库存', dataIndex: 'availableStock', width: 100 },
    { title: '锁定库存', dataIndex: 'lockedStock', width: 100 },
    { title: '已售库存', dataIndex: 'soldStock', width: 100 },
    { title: '预警库存', dataIndex: 'warningStock', width: 100 },
    { title: '状态', dataIndex: 'stockStatus', width: 90 },
    { title: '更新时间', dataIndex: 'updateTime', width: 170 },
    { title: '操作', dataIndex: 'action', width: 120, fixed: 'right' },
  ];

  const recordColumns = [
    { title: '时间', dataIndex: 'createTime', width: 170 },
    { title: '操作类型', dataIndex: 'operationType', width: 110 },
    { title: '变动', dataIndex: 'changeQuantity', width: 90 },
    { title: '变动前可售', dataIndex: 'beforeAvailableStock', width: 110 },
    { title: '变动后可售', dataIndex: 'afterAvailableStock', width: 110 },
    { title: '锁定库存', dataIndex: 'afterLockedStock', width: 100 },
    { title: '已售库存', dataIndex: 'afterSoldStock', width: 100 },
    { title: '备注', dataIndex: 'remark', width: 240 },
    { title: '操作人', dataIndex: 'createUserId', width: 90 },
  ];

  const queryFormDefault = {
    tenantId: 1,
    productId: undefined,
    searchWord: '',
    warningFlag: undefined,
    pageNum: 1,
    pageSize: PAGE_SIZE,
    searchCount: true,
  };
  const queryForm = reactive<any>({ ...queryFormDefault });
  const tableLoading = ref(false);
  const tableData = ref([]);
  const total = ref(0);
  const productOptions = ref<any[]>([]);

  onMounted(async () => {
    await queryProductOptions();
    await queryPage();
  });

  async function queryProductOptions() {
    try {
      const result = await shopProductApi.queryPage({
        tenantId: 1,
        pageNum: 1,
        pageSize: 200,
        searchCount: true,
      });
      productOptions.value = result.data.list || [];
    } catch (error) {
      smartSentry.captureError(error);
    }
  }

  function filterProductOption(input, option) {
    return option.label.toLowerCase().indexOf(input.toLowerCase()) >= 0;
  }

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
      const result = await shopInventoryApi.queryPage(queryForm);
      tableData.value = result.data.list || [];
      total.value = result.data.total || 0;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      tableLoading.value = false;
    }
  }

  const adjustFormRef = ref();
  const adjustVisible = ref(false);
  const currentInventory = reactive<any>({});
  const adjustFormDefault = {
    skuId: undefined,
    changeQuantity: 0,
    warningStock: 0,
    remark: '',
  };
  const adjustForm = reactive<any>({ ...adjustFormDefault });
  const adjustRules = {
    changeQuantity: [{ required: true, message: '请输入调整数量' }],
  };

  function showAdjustModal(record) {
    Object.assign(currentInventory, record);
    Object.assign(adjustForm, {
      skuId: record.skuId,
      changeQuantity: 0,
      warningStock: record.warningStock || 0,
      remark: '',
    });
    adjustVisible.value = true;
  }

  function closeAdjustModal() {
    adjustVisible.value = false;
    Object.assign(adjustForm, adjustFormDefault);
  }

  function submitAdjust() {
    adjustFormRef.value
      .validate()
      .then(async () => {
        if (adjustForm.changeQuantity === 0) {
          message.error('调整数量不能为0');
          return;
        }
        SmartLoading.show();
        try {
          await shopInventoryApi.adjust(adjustForm);
          message.success('库存调整成功');
          closeAdjustModal();
          await queryPage();
        } catch (error) {
          smartSentry.captureError(error);
        } finally {
          SmartLoading.hide();
        }
      })
      .catch(() => {
        message.error('请检查库存调整表单');
      });
  }

  const recordVisible = ref(false);
  const recordLoading = ref(false);
  const recordData = ref([]);
  const recordTotal = ref(0);
  const recordQueryForm = reactive<any>({
    tenantId: 1,
    skuId: undefined,
    pageNum: 1,
    pageSize: PAGE_SIZE,
    searchCount: true,
  });

  function showRecordModal(record) {
    Object.assign(recordQueryForm, {
      tenantId: 1,
      skuId: record.skuId,
      pageNum: 1,
      pageSize: PAGE_SIZE,
      searchCount: true,
    });
    recordVisible.value = true;
    queryRecordPage();
  }

  function closeRecordModal() {
    recordVisible.value = false;
    recordData.value = [];
    recordTotal.value = 0;
  }

  async function queryRecordPage() {
    recordLoading.value = true;
    try {
      const result = await shopInventoryApi.queryRecordPage(recordQueryForm);
      recordData.value = result.data.list || [];
      recordTotal.value = result.data.total || 0;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      recordLoading.value = false;
    }
  }
</script>
