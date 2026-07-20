<template>
  <a-form class="smart-query-form" v-privilege="'shop:order:query'">
    <a-row class="smart-query-form-row">
      <a-form-item label="订单号" class="smart-query-form-item">
        <a-input v-model:value="queryForm.orderNo" style="width: 220px" placeholder="请输入订单号" />
      </a-form-item>

      <a-form-item label="客户" class="smart-query-form-item">
        <a-input v-model:value="queryForm.searchWord" style="width: 220px" placeholder="姓名/邮箱/手机号" />
      </a-form-item>

      <a-form-item label="订单状态" class="smart-query-form-item">
        <a-select v-model:value="queryForm.orderStatus" style="width: 120px" allow-clear placeholder="全部">
          <a-select-option v-for="item in orderStatusOptions" :key="item.value" :value="item.value">{{ item.label }}</a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="支付状态" class="smart-query-form-item">
        <a-select v-model:value="queryForm.payStatus" style="width: 120px" allow-clear placeholder="全部">
          <a-select-option v-for="item in payStatusOptions" :key="item.value" :value="item.value">{{ item.label }}</a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="发货状态" class="smart-query-form-item">
        <a-select v-model:value="queryForm.fulfillmentStatus" style="width: 120px" allow-clear placeholder="全部">
          <a-select-option v-for="item in fulfillmentStatusOptions" :key="item.value" :value="item.value">{{ item.label }}</a-select-option>
        </a-select>
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
      rowKey="orderId"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1500 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'orderNo'">
          <div>{{ record.orderNo }}</div>
          <div class="smart-secondary-text">{{ record.itemCount || 0 }} 件商品</div>
        </template>
        <template v-if="column.dataIndex === 'customerName'">
          <div>{{ record.customerName || '-' }}</div>
          <div class="smart-secondary-text">{{ record.customerEmail || record.customerPhone || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'payableAmountCent'">
          {{ record.currency }} {{ record.payableAmountCent }}
        </template>
        <template v-if="column.dataIndex === 'orderStatus'">
          <a-tag :color="statusColor(orderStatusMap[record.orderStatus])">{{ orderStatusMap[record.orderStatus] || '未知' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'payStatus'">
          <a-tag :color="record.payStatus === 2 ? 'green' : 'default'">{{ payStatusMap[record.payStatus] || '未知' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'fulfillmentStatus'">
          <a-tag :color="record.fulfillmentStatus === 3 ? 'green' : 'blue'">{{ fulfillmentStatusMap[record.fulfillmentStatus] || '未知' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'refundStatus'">
          <a-tag :color="record.refundStatus === 1 ? 'default' : 'orange'">{{ refundStatusMap[record.refundStatus] || '未知' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'action'">
          <div class="smart-table-operate">
            <a-button type="link" @click="showDetail(record.orderId)" v-privilege="'shop:order:detail'">详情</a-button>
            <a-button type="link" @click="showRemarkModal(record)" v-privilege="'shop:order:remark'">备注</a-button>
            <a-button type="link" danger @click="showCancelModal(record)" v-privilege="'shop:order:cancel'">取消</a-button>
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

  <a-modal :open="detailVisible" title="订单详情" :width="980" :footer="null" forceRender @cancel="closeDetail">
    <a-descriptions size="small" bordered :column="2" class="smart-margin-bottom10">
      <a-descriptions-item label="订单号">{{ detail.orderNo }}</a-descriptions-item>
      <a-descriptions-item label="下单时间">{{ detail.createTime }}</a-descriptions-item>
      <a-descriptions-item label="客户">{{ detail.customerName || '-' }}</a-descriptions-item>
      <a-descriptions-item label="联系方式">{{ detail.customerEmail || detail.customerPhone || '-' }}</a-descriptions-item>
      <a-descriptions-item label="订单状态">{{ orderStatusMap[detail.orderStatus] || '-' }}</a-descriptions-item>
      <a-descriptions-item label="支付状态">{{ payStatusMap[detail.payStatus] || '-' }}</a-descriptions-item>
      <a-descriptions-item label="发货状态">{{ fulfillmentStatusMap[detail.fulfillmentStatus] || '-' }}</a-descriptions-item>
      <a-descriptions-item label="退款状态">{{ refundStatusMap[detail.refundStatus] || '-' }}</a-descriptions-item>
      <a-descriptions-item label="收货地址" :span="2">
        {{ detail.countryCode || '' }} {{ detail.province || '' }} {{ detail.city || '' }} {{ detail.addressDetail || '' }}
      </a-descriptions-item>
      <a-descriptions-item label="买家备注" :span="2">{{ detail.buyerRemark || '-' }}</a-descriptions-item>
      <a-descriptions-item label="商家备注" :span="2">{{ detail.sellerRemark || '-' }}</a-descriptions-item>
      <a-descriptions-item label="取消原因" :span="2" v-if="detail.cancelReason">{{ detail.cancelReason }}</a-descriptions-item>
    </a-descriptions>

    <a-table size="small" :dataSource="detail.itemList || []" :columns="itemColumns" rowKey="orderItemId" bordered :pagination="false">
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'productName'">
          <div>{{ record.productName }}</div>
          <div class="smart-secondary-text">{{ record.skuName }} / {{ record.specSummary || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'salePriceCent'">
          {{ record.currency }} {{ record.salePriceCent }}
        </template>
        <template v-if="column.dataIndex === 'totalAmountCent'">
          {{ record.currency }} {{ record.totalAmountCent }}
        </template>
      </template>
    </a-table>

    <a-descriptions size="small" bordered :column="2" class="smart-margin-top10">
      <a-descriptions-item label="商品金额">{{ detail.currency }} {{ detail.productAmountCent }}</a-descriptions-item>
      <a-descriptions-item label="运费">{{ detail.currency }} {{ detail.freightAmountCent }}</a-descriptions-item>
      <a-descriptions-item label="优惠">{{ detail.currency }} {{ detail.discountAmountCent }}</a-descriptions-item>
      <a-descriptions-item label="税费">{{ detail.currency }} {{ detail.taxAmountCent }}</a-descriptions-item>
      <a-descriptions-item label="应付金额">{{ detail.currency }} {{ detail.payableAmountCent }}</a-descriptions-item>
      <a-descriptions-item label="实付金额">{{ detail.currency }} {{ detail.paidAmountCent }}</a-descriptions-item>
    </a-descriptions>
  </a-modal>

  <a-modal
    :open="remarkVisible"
    title="商家备注"
    :width="560"
    forceRender
    ok-text="确认"
    cancel-text="取消"
    @ok="submitRemark"
    @cancel="closeRemarkModal"
  >
    <a-form ref="remarkFormRef" :model="remarkForm" :label-col="{ span: 4 }" :wrapper-col="{ span: 18 }">
      <a-form-item label="备注" name="sellerRemark">
        <a-textarea v-model:value="remarkForm.sellerRemark" :rows="4" placeholder="请输入商家内部备注" />
      </a-form-item>
    </a-form>
  </a-modal>

  <a-modal
    :open="cancelVisible"
    title="取消订单"
    :width="560"
    forceRender
    ok-text="确认取消"
    cancel-text="返回"
    @ok="submitCancel"
    @cancel="closeCancelModal"
  >
    <a-form ref="cancelFormRef" :model="cancelForm" :rules="cancelRules" :label-col="{ span: 5 }" :wrapper-col="{ span: 17 }">
      <a-form-item label="订单号">
        <a-input :value="currentOrder.orderNo" disabled />
      </a-form-item>
      <a-form-item label="取消原因" name="cancelReason">
        <a-textarea v-model:value="cancelForm.cancelReason" :rows="4" placeholder="请输入取消原因" />
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
  import { shopOrderApi } from '/@/api/business/shop/shop-order-api';

  const orderStatusOptions = [
    { value: 1, label: '待支付' },
    { value: 2, label: '已确认' },
    { value: 3, label: '已发货' },
    { value: 4, label: '已完成' },
    { value: 5, label: '已取消' },
  ];
  const payStatusOptions = [
    { value: 1, label: '未支付' },
    { value: 2, label: '已支付' },
  ];
  const fulfillmentStatusOptions = [
    { value: 1, label: '未发货' },
    { value: 2, label: '部分发货' },
    { value: 3, label: '已发货' },
  ];
  const refundStatusOptions = [
    { value: 1, label: '无退款' },
    { value: 2, label: '退款中' },
    { value: 3, label: '已退款' },
  ];
  const orderStatusMap = toLabelMap(orderStatusOptions);
  const payStatusMap = toLabelMap(payStatusOptions);
  const fulfillmentStatusMap = toLabelMap(fulfillmentStatusOptions);
  const refundStatusMap = toLabelMap(refundStatusOptions);

  const columns = [
    { title: '订单号', dataIndex: 'orderNo', width: 210 },
    { title: '客户', dataIndex: 'customerName', width: 220 },
    { title: '应付金额', dataIndex: 'payableAmountCent', width: 120 },
    { title: '订单状态', dataIndex: 'orderStatus', width: 110 },
    { title: '支付状态', dataIndex: 'payStatus', width: 110 },
    { title: '发货状态', dataIndex: 'fulfillmentStatus', width: 110 },
    { title: '退款状态', dataIndex: 'refundStatus', width: 110 },
    { title: '下单时间', dataIndex: 'createTime', width: 170 },
    { title: '商家备注', dataIndex: 'sellerRemark', width: 240 },
    { title: '操作', dataIndex: 'action', width: 150, fixed: 'right' },
  ];

  const itemColumns = [
    { title: '商品', dataIndex: 'productName', width: 320 },
    { title: 'SKU编码', dataIndex: 'skuCode', width: 150 },
    { title: '单价(分)', dataIndex: 'salePriceCent', width: 120 },
    { title: '数量', dataIndex: 'quantity', width: 90 },
    { title: '小计(分)', dataIndex: 'totalAmountCent', width: 120 },
  ];

  const queryFormDefault = {
    tenantId: 1,
    orderNo: '',
    searchWord: '',
    orderStatus: undefined,
    payStatus: undefined,
    fulfillmentStatus: undefined,
    refundStatus: undefined,
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
      const result = await shopOrderApi.queryPage(queryForm);
      tableData.value = result.data.list || [];
      total.value = result.data.total || 0;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      tableLoading.value = false;
    }
  }

  const detailVisible = ref(false);
  const detail = reactive<any>({});

  async function showDetail(orderId) {
    SmartLoading.show();
    try {
      const result = await shopOrderApi.detail(orderId);
      Object.assign(detail, result.data || {});
      detailVisible.value = true;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function closeDetail() {
    detailVisible.value = false;
    Object.assign(detail, {});
  }

  const remarkFormRef = ref();
  const remarkVisible = ref(false);
  const remarkForm = reactive<any>({
    orderId: undefined,
    sellerRemark: '',
  });

  function showRemarkModal(record) {
    Object.assign(remarkForm, {
      orderId: record.orderId,
      sellerRemark: record.sellerRemark || '',
    });
    remarkVisible.value = true;
  }

  function closeRemarkModal() {
    remarkVisible.value = false;
    Object.assign(remarkForm, { orderId: undefined, sellerRemark: '' });
  }

  async function submitRemark() {
    SmartLoading.show();
    try {
      await shopOrderApi.updateRemark(remarkForm);
      message.success('备注更新成功');
      closeRemarkModal();
      await queryPage();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  const cancelFormRef = ref();
  const cancelVisible = ref(false);
  const currentOrder = reactive<any>({});
  const cancelForm = reactive<any>({
    orderId: undefined,
    cancelReason: '',
  });
  const cancelRules = {
    cancelReason: [{ required: true, message: '请输入取消原因' }],
  };

  function showCancelModal(record) {
    Object.assign(currentOrder, record);
    Object.assign(cancelForm, {
      orderId: record.orderId,
      cancelReason: '',
    });
    cancelVisible.value = true;
  }

  function closeCancelModal() {
    cancelVisible.value = false;
    Object.assign(currentOrder, {});
    Object.assign(cancelForm, { orderId: undefined, cancelReason: '' });
  }

  function submitCancel() {
    cancelFormRef.value
      .validate()
      .then(async () => {
        SmartLoading.show();
        try {
          await shopOrderApi.cancel(cancelForm);
          message.success('订单已取消');
          closeCancelModal();
          await queryPage();
        } catch (error) {
          smartSentry.captureError(error);
        } finally {
          SmartLoading.hide();
        }
      })
      .catch(() => {
        message.error('请填写取消原因');
      });
  }

  function toLabelMap(options) {
    return options.reduce((map, item) => {
      map[item.value] = item.label;
      return map;
    }, {});
  }

  function statusColor(label) {
    if (label === '已取消') {
      return 'red';
    }
    if (label === '已完成') {
      return 'green';
    }
    if (label === '已发货') {
      return 'blue';
    }
    return 'default';
  }
</script>
