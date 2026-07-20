<template>
  <a-form class="smart-query-form" v-privilege="'shop:cms:query'">
    <a-row class="smart-query-form-row">
      <a-form-item label="区块类型" class="smart-query-form-item">
        <a-radio-group v-model:value="queryForm.blockType" @change="onSearch">
          <a-radio-button :value="undefined">全部</a-radio-button>
          <a-radio-button v-for="item in blockTypeOptions" :key="item.value" :value="item.value">{{ item.label }}</a-radio-button>
        </a-radio-group>
      </a-form-item>

      <a-form-item label="关键字" class="smart-query-form-item">
        <a-input v-model:value="queryForm.searchWord" style="width: 240px" placeholder="区块名称/标题/商品" />
      </a-form-item>

      <a-form-item label="状态" class="smart-query-form-item">
        <a-select v-model:value="queryForm.disabledFlag" style="width: 120px" allow-clear placeholder="全部">
          <a-select-option :value="false">启用</a-select-option>
          <a-select-option :value="true">禁用</a-select-option>
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
    <a-row class="smart-table-btn-block">
      <div class="smart-table-operate-block">
        <a-button type="primary" @click="showForm()" v-privilege="'shop:cms:add'">
          <template #icon>
            <PlusOutlined />
          </template>
          新增区块
        </a-button>
      </div>
    </a-row>

    <a-table
      size="small"
      :dataSource="tableData"
      :columns="columns"
      rowKey="blockId"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1350 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'blockType'">
          <a-tag color="blue">{{ blockTypeMap[record.blockType] || '-' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'blockName'">
          <div>{{ record.blockName }}</div>
          <div class="smart-secondary-text">{{ record.blockTitle || '-' }}</div>
        </template>
        <template v-if="column.dataIndex === 'image'">
          <a-image v-if="getImageUrl(record.image)" :src="getImageUrl(record.image)" :width="72" :height="42" />
          <span v-else>-</span>
        </template>
        <template v-if="column.dataIndex === 'productName'">
          {{ record.productName || '-' }}
        </template>
        <template v-if="column.dataIndex === 'disabledFlag'">
          <a-switch
            v-privilege="'shop:cms:update'"
            :checked="!record.disabledFlag"
            checked-children="启用"
            un-checked-children="禁用"
            @change="(checked) => updateDisabled(record, checked)"
          />
        </template>
        <template v-if="column.dataIndex === 'action'">
          <div class="smart-table-operate">
            <a-button type="link" @click="showForm(record)" v-privilege="'shop:cms:update'">编辑</a-button>
            <a-button type="link" danger @click="confirmDelete(record)" v-privilege="'shop:cms:delete'">删除</a-button>
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
    :open="formVisible"
    :title="form.blockId ? '编辑CMS区块' : '新增CMS区块'"
    :width="760"
    forceRender
    ok-text="确认"
    cancel-text="取消"
    @ok="submit"
    @cancel="closeForm"
  >
    <a-form ref="formRef" :model="form" :rules="rules" :label-col="{ span: 5 }" :wrapper-col="{ span: 17 }">
      <a-form-item label="区块类型" name="blockType">
        <a-select v-model:value="form.blockType" placeholder="请选择区块类型" @change="onBlockTypeChange">
          <a-select-option v-for="item in blockTypeOptions" :key="item.value" :value="item.value">{{ item.label }}</a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="区块名称" name="blockName">
        <a-input v-model:value="form.blockName" placeholder="内部识别名称，例如：首页主Banner" />
      </a-form-item>

      <a-form-item label="展示标题" name="blockTitle">
        <a-input v-model:value="form.blockTitle" placeholder="展示给C端用户的标题" />
      </a-form-item>

      <a-form-item label="展示副标题" name="blockSubTitle">
        <a-input v-model:value="form.blockSubTitle" placeholder="可选，适合Banner副标题" />
      </a-form-item>

      <a-form-item v-if="form.blockType === BLOCK_TYPE_RECOMMEND_PRODUCT" label="推荐商品" name="productId">
        <a-select
          v-model:value="form.productId"
          style="width: 100%"
          show-search
          allow-clear
          :filter-option="filterProductOption"
          placeholder="请选择推荐商品"
        >
          <a-select-option v-for="item in productOptions" :key="item.productId" :value="item.productId" :label="item.productName">
            {{ item.productName }}
          </a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="图片" name="image">
        <Upload
          accept=".jpg,.jpeg,.png,.gif"
          :maxUploadSize="1"
          :maxSize="5"
          buttonText="上传图片"
          :default-file-list="form.image"
          @change="imageChange"
        />
      </a-form-item>

      <a-form-item label="跳转链接" name="linkUrl">
        <a-input v-model:value="form.linkUrl" placeholder="例如 /products/1001 或 https://example.com" />
      </a-form-item>

      <a-row>
        <a-col :span="12">
          <a-form-item label="排序" name="sort" :label-col="{ span: 10 }" :wrapper-col="{ span: 12 }">
            <a-input-number v-model:value="form.sort" style="width: 100%" :min="0" />
          </a-form-item>
        </a-col>
        <a-col :span="12">
          <a-form-item label="状态" name="disabledFlag" :label-col="{ span: 8 }" :wrapper-col="{ span: 14 }">
            <a-switch v-model:checked="form.enabledFlag" checked-children="启用" un-checked-children="禁用" />
          </a-form-item>
        </a-col>
      </a-row>

      <a-form-item label="扩展配置" name="configJson">
        <a-textarea v-model:value="form.configJson" :rows="3" placeholder="可选，填写JSON配置，例如 {&quot;layout&quot;:&quot;wide&quot;}" />
      </a-form-item>
    </a-form>
  </a-modal>
</template>

<script setup lang="ts">
  import { onMounted, reactive, ref } from 'vue';
  import { message, Modal } from 'ant-design-vue';
  import Upload from '/@/components/support/file-upload/index.vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { PAGE_SIZE, PAGE_SIZE_OPTIONS, showTableTotal } from '/@/constants/common-const';
  import { shopCmsApi } from '/@/api/business/shop/shop-cms-api';
  import { shopProductApi } from '/@/api/business/shop/shop-product-api';

  const BLOCK_TYPE_BANNER = 1;
  const BLOCK_TYPE_NAVIGATION = 2;
  const BLOCK_TYPE_RECOMMEND_PRODUCT = 3;

  const blockTypeOptions = [
    { value: BLOCK_TYPE_BANNER, label: '首页Banner' },
    { value: BLOCK_TYPE_NAVIGATION, label: '导航菜单' },
    { value: BLOCK_TYPE_RECOMMEND_PRODUCT, label: '推荐商品' },
  ];
  const blockTypeMap = blockTypeOptions.reduce((map, item) => {
    map[item.value] = item.label;
    return map;
  }, {});

  const columns = [
    { title: '类型', dataIndex: 'blockType', width: 120 },
    { title: '区块', dataIndex: 'blockName', width: 240 },
    { title: '图片', dataIndex: 'image', width: 110 },
    { title: '推荐商品', dataIndex: 'productName', width: 220 },
    { title: '链接', dataIndex: 'linkUrl', width: 260 },
    { title: '排序', dataIndex: 'sort', width: 80 },
    { title: '状态', dataIndex: 'disabledFlag', width: 100 },
    { title: '更新时间', dataIndex: 'updateTime', width: 170 },
    { title: '操作', dataIndex: 'action', width: 120, fixed: 'right' },
  ];

  const queryFormDefault = {
    tenantId: 1,
    blockType: undefined,
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
      const result = await shopCmsApi.queryPage(queryForm);
      tableData.value = result.data.list || [];
      total.value = result.data.total || 0;
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      tableLoading.value = false;
    }
  }

  const formRef = ref();
  const formVisible = ref(false);
  const formDefault = {
    blockId: undefined,
    tenantId: 1,
    blockType: BLOCK_TYPE_BANNER,
    blockName: '',
    blockTitle: '',
    blockSubTitle: '',
    image: [],
    linkUrl: '',
    productId: undefined,
    configJson: '',
    sort: 0,
    enabledFlag: true,
  };
  const form = reactive<any>({ ...formDefault });
  const rules = {
    blockType: [{ required: true, message: '请选择区块类型' }],
    blockName: [{ required: true, message: '请输入区块名称' }],
    productId: [{ required: true, message: '请选择推荐商品' }],
  };

  function showForm(rowData?) {
    Object.assign(form, formDefault);
    if (rowData) {
      Object.assign(form, rowData, {
        image: rowData.image || [],
        enabledFlag: !rowData.disabledFlag,
      });
    }
    formVisible.value = true;
  }

  function closeForm() {
    formVisible.value = false;
    Object.assign(form, formDefault);
  }

  function onBlockTypeChange() {
    if (form.blockType !== BLOCK_TYPE_RECOMMEND_PRODUCT) {
      form.productId = undefined;
    }
  }

  function submit() {
    formRef.value
      .validate()
      .then(async () => {
        SmartLoading.show();
        try {
          const submitForm = {
            ...form,
            disabledFlag: !form.enabledFlag,
          };
          if (form.blockId) {
            await shopCmsApi.update(submitForm);
          } else {
            await shopCmsApi.add(submitForm);
          }
          message.success(`${form.blockId ? '修改' : '新增'}成功`);
          closeForm();
          await queryPage();
        } catch (error) {
          smartSentry.captureError(error);
        } finally {
          SmartLoading.hide();
        }
      })
      .catch(() => {
        message.error('请检查CMS区块表单');
      });
  }

  async function updateDisabled(record, checked) {
    SmartLoading.show();
    try {
      await shopCmsApi.updateDisabled({
        blockId: record.blockId,
        disabledFlag: !checked,
      });
      message.success(checked ? '区块已启用' : '区块已禁用');
      await queryPage();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function confirmDelete(record) {
    Modal.confirm({
      title: '提示',
      content: `确定删除「${record.blockName}」吗？`,
      okText: '确定',
      okType: 'danger',
      cancelText: '取消',
      async onOk() {
        await deleteBlock(record.blockId);
      },
    });
  }

  async function deleteBlock(blockId) {
    SmartLoading.show();
    try {
      await shopCmsApi.delete(blockId);
      message.success('删除成功');
      await queryPage();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function imageChange(fileList) {
    form.image = fileList;
  }

  function getImageUrl(imageList) {
    if (!imageList || imageList.length === 0) {
      return '';
    }
    return imageList[0].fileUrl || imageList[0].url || '';
  }
</script>
