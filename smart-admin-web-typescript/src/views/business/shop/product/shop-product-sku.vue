<template>
  <a-card size="small" :bordered="false">
    <a-row class="smart-query-form-row" align="middle">
      <a-form-item label="商品" class="smart-query-form-item">
        <a-select
          v-model:value="productId"
          style="width: 320px"
          show-search
          allow-clear
          :filter-option="filterProductOption"
          placeholder="请选择商品"
          @change="onProductChange"
        >
          <a-select-option v-for="item in productOptions" :key="item.productId" :value="item.productId" :label="item.productName">
            {{ item.productName }}
          </a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item class="smart-query-form-item">
        <a-button-group>
          <a-button type="primary" @click="querySkuList" v-privilege="'shop:sku:query'">
            <template #icon>
              <SearchOutlined />
            </template>
            查询
          </a-button>
          <a-button @click="goProductList">
            <template #icon>
              <RollbackOutlined />
            </template>
            返回商品
          </a-button>
        </a-button-group>
      </a-form-item>
    </a-row>
  </a-card>

  <a-card size="small" :bordered="false" class="smart-margin-top10">
    <a-row class="smart-table-btn-block">
      <div class="smart-table-operate-block">
        <a-button @click="addSpecGroup" v-privilege="'shop:sku:update'">
          <template #icon>
            <PlusOutlined />
          </template>
          规格组
        </a-button>
        <a-button @click="generateSkuList" v-privilege="'shop:sku:update'">生成SKU</a-button>
        <a-button type="primary" @click="saveSkuList" v-privilege="'shop:sku:update'">
          <template #icon>
            <SaveOutlined />
          </template>
          保存SKU
        </a-button>
      </div>
    </a-row>

    <a-table size="small" :dataSource="specGroupList" :columns="specColumns" rowKey="key" bordered :pagination="false">
      <template #bodyCell="{ record, column, index }">
        <template v-if="column.dataIndex === 'specName'">
          <a-input v-model:value="record.specName" placeholder="如：颜色" />
        </template>
        <template v-if="column.dataIndex === 'specValues'">
          <a-input v-model:value="record.specValues" placeholder="多个值用逗号分隔，如：红色,蓝色" />
        </template>
        <template v-if="column.dataIndex === 'action'">
          <a-button type="link" danger @click="removeSpecGroup(index)">删除</a-button>
        </template>
      </template>
    </a-table>
  </a-card>

  <a-card size="small" :bordered="false" class="smart-margin-top10">
    <a-table
      size="small"
      :dataSource="skuList"
      :columns="skuColumns"
      rowKey="rowKey"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1500 }"
    >
      <template #bodyCell="{ record, column, index }">
        <template v-if="column.dataIndex === 'skuName'">
          <a-input v-model:value="record.skuName" placeholder="SKU名称" />
        </template>
        <template v-if="column.dataIndex === 'skuCode'">
          <a-input v-model:value="record.skuCode" placeholder="SKU编码" />
        </template>
        <template v-if="column.dataIndex === 'specSummary'">
          <a-input v-model:value="record.specSummary" placeholder="规格摘要" />
        </template>
        <template v-if="column.dataIndex === 'skuImage'">
          <Upload
            accept=".jpg,.jpeg,.png,.gif"
            :maxUploadSize="1"
            :maxSize="5"
            buttonText="上传"
            :default-file-list="record.skuImage"
            @change="(fileList) => skuImageChange(record, fileList)"
          />
        </template>
        <template v-if="column.dataIndex === 'salePriceCent'">
          <a-input-number v-model:value="record.salePriceCent" style="width: 100%" :min="0" />
        </template>
        <template v-if="column.dataIndex === 'marketPriceCent'">
          <a-input-number v-model:value="record.marketPriceCent" style="width: 100%" :min="0" />
        </template>
        <template v-if="column.dataIndex === 'costPriceCent'">
          <a-input-number v-model:value="record.costPriceCent" style="width: 100%" :min="0" />
        </template>
        <template v-if="column.dataIndex === 'availableStock'">
          <a-input-number v-model:value="record.availableStock" style="width: 100%" :min="0" />
        </template>
        <template v-if="column.dataIndex === 'warningStock'">
          <a-input-number v-model:value="record.warningStock" style="width: 100%" :min="0" />
        </template>
        <template v-if="column.dataIndex === 'disabledFlag'">
          <a-switch v-model:checked="record.disabledFlag" checked-children="禁用" un-checked-children="启用" />
        </template>
        <template v-if="column.dataIndex === 'action'">
          <a-button type="link" danger @click="confirmDeleteSku(record, index)" v-privilege="'shop:sku:update'">删除</a-button>
        </template>
      </template>
    </a-table>
  </a-card>
</template>

<script setup lang="ts">
  import { onMounted, ref } from 'vue';
  import { useRoute, useRouter } from 'vue-router';
  import { message, Modal } from 'ant-design-vue';
  import Upload from '/@/components/support/file-upload/index.vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { shopProductApi } from '/@/api/business/shop/shop-product-api';
  import { shopProductSkuApi } from '/@/api/business/shop/shop-product-sku-api';

  const route = useRoute();
  const router = useRouter();

  const productId = ref<any>(route.query.productId ? Number(route.query.productId) : undefined);
  const productOptions = ref<any[]>([]);
  const tableLoading = ref(false);
  const specGroupList = ref<any[]>([
    { key: Date.now(), specName: '颜色', specValues: '' },
    { key: Date.now() + 1, specName: '尺码', specValues: '' },
  ]);
  const skuList = ref<any[]>([]);

  const specColumns = [
    { title: '规格名', dataIndex: 'specName', width: 220 },
    { title: '规格值', dataIndex: 'specValues' },
    { title: '操作', dataIndex: 'action', width: 100 },
  ];

  const skuColumns = [
    { title: 'SKU名称', dataIndex: 'skuName', width: 180 },
    { title: 'SKU编码', dataIndex: 'skuCode', width: 150 },
    { title: '规格', dataIndex: 'specSummary', width: 180 },
    { title: '图片', dataIndex: 'skuImage', width: 150 },
    { title: '售价(分)', dataIndex: 'salePriceCent', width: 120 },
    { title: '市场价(分)', dataIndex: 'marketPriceCent', width: 120 },
    { title: '成本价(分)', dataIndex: 'costPriceCent', width: 120 },
    { title: '可售库存', dataIndex: 'availableStock', width: 110 },
    { title: '预警库存', dataIndex: 'warningStock', width: 110 },
    { title: '禁用', dataIndex: 'disabledFlag', width: 90 },
    { title: '操作', dataIndex: 'action', width: 90 },
  ];

  onMounted(async () => {
    await queryProductOptions();
    if (productId.value) {
      await querySkuList();
    }
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

  function onProductChange() {
    skuList.value = [];
    if (productId.value) {
      querySkuList();
    }
  }

  async function querySkuList() {
    if (!productId.value) {
      message.warning('请先选择商品');
      return;
    }
    tableLoading.value = true;
    try {
      const result = await shopProductSkuApi.queryList({ productId: productId.value });
      skuList.value = (result.data || []).map((item) => ({
        ...item,
        rowKey: item.skuId || createRowKey(),
      }));
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      tableLoading.value = false;
    }
  }

  function addSpecGroup() {
    specGroupList.value.push({
      key: createRowKey(),
      specName: '',
      specValues: '',
    });
  }

  function removeSpecGroup(index) {
    specGroupList.value.splice(index, 1);
  }

  function generateSkuList() {
    if (!productId.value) {
      message.warning('请先选择商品');
      return;
    }
    const specGroups = specGroupList.value
      .map((item) => ({
        specName: item.specName?.trim(),
        values: splitSpecValues(item.specValues),
      }))
      .filter((item) => item.specName && item.values.length > 0);
    if (specGroups.length === 0) {
      message.warning('请至少填写一个规格组和规格值');
      return;
    }

    const combinations = buildCombinations(specGroups);
    const defaultCurrency = currentProduct()?.currency || 'USD';
    const defaultPrice = currentProduct()?.salePriceCent || 0;
    skuList.value = combinations.map((combination, index) => {
      const specSummary = combination.map((item) => `${item.specName}:${item.specValue}`).join('; ');
      return {
        rowKey: createRowKey(),
        productId: productId.value,
        skuName: `${currentProduct()?.productName || 'SKU'} ${combination.map((item) => item.specValue).join(' ')}`,
        skuCode: '',
        specJson: JSON.stringify(combination),
        specSummary,
        skuImage: [],
        salePriceCent: defaultPrice,
        marketPriceCent: defaultPrice,
        costPriceCent: 0,
        currency: defaultCurrency,
        availableStock: 0,
        warningStock: 0,
        disabledFlag: false,
        sort: index,
      };
    });
  }

  async function saveSkuList() {
    if (!productId.value) {
      message.warning('请先选择商品');
      return;
    }
    if (skuList.value.length === 0) {
      message.warning('请先生成或录入SKU');
      return;
    }
    const errorMessage = validateSkuList();
    if (errorMessage) {
      message.error(errorMessage);
      return;
    }

    SmartLoading.show();
    try {
      await shopProductSkuApi.saveList({
        productId: productId.value,
        skuList: skuList.value.map((item, index) => ({
          skuId: item.skuId,
          productId: productId.value,
          skuName: item.skuName,
          skuCode: item.skuCode,
          specJson: item.specJson,
          specSummary: item.specSummary,
          skuImage: item.skuImage,
          salePriceCent: item.salePriceCent,
          marketPriceCent: item.marketPriceCent,
          costPriceCent: item.costPriceCent,
          currency: item.currency || currentProduct()?.currency || 'USD',
          availableStock: item.availableStock,
          warningStock: item.warningStock,
          disabledFlag: item.disabledFlag,
          sort: index,
        })),
      });
      message.success('SKU保存成功');
      await querySkuList();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function confirmDeleteSku(record, index) {
    if (!record.skuId) {
      skuList.value.splice(index, 1);
      return;
    }
    Modal.confirm({
      title: '提示',
      content: `确定要删除「${record.skuName}」吗？`,
      okText: '确定',
      okType: 'danger',
      cancelText: '取消',
      async onOk() {
        await deleteSku(record.skuId);
      },
    });
  }

  async function deleteSku(skuId) {
    SmartLoading.show();
    try {
      await shopProductSkuApi.delete(skuId);
      message.success('删除成功');
      await querySkuList();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function skuImageChange(record, fileList) {
    record.skuImage = fileList;
  }

  function splitSpecValues(value) {
    if (!value) {
      return [];
    }
    return value
      .split(/[,，]/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function buildCombinations(specGroups) {
    return specGroups.reduce(
      (result, group) =>
        result.flatMap((item) =>
          group.values.map((value) => [
            ...item,
            {
              specName: group.specName,
              specValue: value,
            },
          ])
        ),
      [[]]
    );
  }

  function validateSkuList() {
    const skuCodeSet = new Set();
    for (const item of skuList.value) {
      if (!item.skuName) {
        return 'SKU名称不能为空';
      }
      if (item.salePriceCent === undefined || item.salePriceCent === null) {
        return 'SKU售价不能为空';
      }
      if (item.availableStock === undefined || item.availableStock === null) {
        return 'SKU可售库存不能为空';
      }
      if (item.warningStock === undefined || item.warningStock === null) {
        return 'SKU预警库存不能为空';
      }
      if (item.skuCode) {
        if (skuCodeSet.has(item.skuCode)) {
          return `SKU编码重复：${item.skuCode}`;
        }
        skuCodeSet.add(item.skuCode);
      }
    }
    return '';
  }

  function currentProduct() {
    return productOptions.value.find((item) => item.productId === productId.value);
  }

  function createRowKey() {
    return `${Date.now()}_${Math.random()}`;
  }

  function goProductList() {
    router.push({ path: '/shop/product' });
  }
</script>
