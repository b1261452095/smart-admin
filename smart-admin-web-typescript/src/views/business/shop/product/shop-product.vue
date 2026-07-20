<template>
  <a-form class="smart-query-form" v-privilege="'shop:product:query'">
    <a-row class="smart-query-form-row">
      <a-form-item label="关键字" class="smart-query-form-item">
        <a-input v-model:value="queryForm.searchWord" style="width: 220px" placeholder="商品名称/编码" />
      </a-form-item>

      <a-form-item label="商品类目" class="smart-query-form-item">
        <a-tree-select
          v-model:value="queryForm.categoryId"
          style="width: 220px"
          allow-clear
          tree-default-expand-all
          :tree-data="categoryTree"
          placeholder="请选择类目"
        />
      </a-form-item>

      <a-form-item label="发布状态" class="smart-query-form-item">
        <a-select v-model:value="queryForm.publishStatus" style="width: 120px" allow-clear placeholder="全部">
          <a-select-option :value="1">草稿</a-select-option>
          <a-select-option :value="2">已发布</a-select-option>
        </a-select>
      </a-form-item>

      <a-form-item label="上架状态" class="smart-query-form-item">
        <a-radio-group v-model:value="queryForm.shelvesFlag" @change="onSearch">
          <a-radio-button :value="undefined">全部</a-radio-button>
          <a-radio-button :value="true">上架</a-radio-button>
          <a-radio-button :value="false">下架</a-radio-button>
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

  <a-card size="small" :bordered="false" :hoverable="true">
    <a-row class="smart-table-btn-block">
      <div class="smart-table-operate-block">
        <a-button type="primary" @click="showForm()" v-privilege="'shop:product:add'">
          <template #icon>
            <PlusOutlined />
          </template>
          新建商品
        </a-button>
      </div>
    </a-row>

    <a-table
      size="small"
      :dataSource="tableData"
      :columns="columns"
      rowKey="productId"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1300 }"
    >
      <template #bodyCell="{ record, column, text }">
        <template v-if="column.dataIndex === 'publishStatus'">
          <a-tag :color="text === 2 ? 'green' : 'default'">{{ text === 2 ? '已发布' : '草稿' }}</a-tag>
        </template>
        <template v-if="column.dataIndex === 'shelvesFlag'">
          <a-switch
            :checked="record.shelvesFlag"
            checked-children="上架"
            un-checked-children="下架"
            @change="(checked) => updateShelves(record, checked)"
            v-privilege="'shop:product:shelve'"
          />
        </template>
        <template v-if="column.dataIndex === 'salePriceCent'">
          {{ record.currency }} {{ record.salePriceCent }}
        </template>
        <template v-if="column.dataIndex === 'action'">
          <div class="smart-table-operate">
            <a-button type="link" @click="showSku(record)" v-privilege="'shop:sku:query'">SKU</a-button>
            <a-button type="link" @click="showForm(undefined, record)" v-privilege="'shop:product:update'">编辑</a-button>
            <a-button type="link" danger @click="confirmDelete(record)" v-privilege="'shop:product:delete'">删除</a-button>
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
    :title="form.productId ? '编辑商品' : '新增商品'"
    :width="820"
    forceRender
    ok-text="确认"
    cancel-text="取消"
    @ok="submit"
    @cancel="closeForm"
  >
    <a-form ref="formRef" :model="form" :rules="rules" :label-col="{ span: 4 }" :wrapper-col="{ span: 18 }">
      <a-form-item label="商品类目" name="categoryId">
        <a-tree-select
          v-model:value="form.categoryId"
          style="width: 100%"
          tree-default-expand-all
          :tree-data="categoryTree"
          placeholder="请选择类目"
        />
      </a-form-item>

      <a-form-item label="商品名称" name="productName">
        <a-input v-model:value="form.productName" placeholder="请输入商品名称" />
      </a-form-item>

      <a-form-item label="商品编码" name="productCode">
        <a-input v-model:value="form.productCode" placeholder="可选，建议唯一" />
      </a-form-item>

      <a-form-item label="副标题" name="subTitle">
        <a-input v-model:value="form.subTitle" placeholder="请输入商品副标题" />
      </a-form-item>

      <a-form-item label="主图" name="mainImage">
        <Upload
          accept=".jpg,.jpeg,.png,.gif"
          :maxUploadSize="1"
          :maxSize="5"
          buttonText="上传主图"
          :default-file-list="form.mainImage"
          @change="mainImageChange"
        />
      </a-form-item>

      <a-form-item label="详情图片" name="detailImages">
        <Upload
          accept=".jpg,.jpeg,.png,.gif"
          :maxUploadSize="8"
          :maxSize="5"
          buttonText="上传详情图"
          :default-file-list="form.detailImages"
          @change="detailImagesChange"
        />
      </a-form-item>

      <a-row>
        <a-col :span="12">
          <a-form-item label="价格(分)" name="salePriceCent" :label-col="{ span: 8 }" :wrapper-col="{ span: 14 }">
            <a-input-number v-model:value="form.salePriceCent" style="width: 100%" :min="0" />
          </a-form-item>
        </a-col>
        <a-col :span="12">
          <a-form-item label="币种" name="currency" :label-col="{ span: 8 }" :wrapper-col="{ span: 14 }">
            <a-select v-model:value="form.currency">
              <a-select-option value="USD">USD</a-select-option>
              <a-select-option value="CNY">CNY</a-select-option>
              <a-select-option value="EUR">EUR</a-select-option>
            </a-select>
          </a-form-item>
        </a-col>
      </a-row>

      <a-row>
        <a-col :span="12">
          <a-form-item label="发布状态" name="publishStatus" :label-col="{ span: 8 }" :wrapper-col="{ span: 14 }">
            <a-select v-model:value="form.publishStatus">
              <a-select-option :value="1">草稿</a-select-option>
              <a-select-option :value="2">已发布</a-select-option>
            </a-select>
          </a-form-item>
        </a-col>
        <a-col :span="12">
          <a-form-item label="排序" name="sort" :label-col="{ span: 8 }" :wrapper-col="{ span: 14 }">
            <a-input-number v-model:value="form.sort" style="width: 100%" :min="0" />
          </a-form-item>
        </a-col>
      </a-row>

      <a-form-item label="上架状态" name="shelvesFlag">
        <a-switch v-model:checked="form.shelvesFlag" checked-children="上架" un-checked-children="下架" />
      </a-form-item>

      <a-form-item label="SEO标题" name="seoTitle">
        <a-input v-model:value="form.seoTitle" placeholder="请输入SEO标题" />
      </a-form-item>

      <a-form-item label="SEO描述" name="seoDescription">
        <a-textarea v-model:value="form.seoDescription" :rows="2" placeholder="请输入SEO描述" />
      </a-form-item>

      <a-form-item label="商品详情" name="productDetail">
        <a-textarea v-model:value="form.productDetail" :rows="4" placeholder="阶段三先用文本详情，后续可换富文本" />
      </a-form-item>

      <a-form-item label="备注" name="remark">
        <a-textarea v-model:value="form.remark" :rows="2" placeholder="内部备注，不展示给用户" />
      </a-form-item>
    </a-form>
  </a-modal>
</template>

<script setup lang="ts">
  import { onMounted, reactive, ref } from 'vue';
  import { useRouter } from 'vue-router';
  import { message, Modal } from 'ant-design-vue';
  import Upload from '/@/components/support/file-upload/index.vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { PAGE_SIZE, PAGE_SIZE_OPTIONS, showTableTotal } from '/@/constants/common-const';
  import { shopCategoryApi } from '/@/api/business/shop/shop-category-api';
  import { shopProductApi } from '/@/api/business/shop/shop-product-api';

  const columns = [
    {
      title: '商品名称',
      dataIndex: 'productName',
      minWidth: 200,
    },
    {
      title: '商品编码',
      dataIndex: 'productCode',
      width: 150,
    },
    {
      title: '类目',
      dataIndex: 'categoryName',
      width: 190,
    },
    {
      title: '价格(分)',
      dataIndex: 'salePriceCent',
      width: 120,
    },
    {
      title: '发布状态',
      dataIndex: 'publishStatus',
      width: 100,
    },
    {
      title: '上架',
      dataIndex: 'shelvesFlag',
      width: 100,
    },
    {
      title: '排序',
      dataIndex: 'sort',
      width: 80,
    },
    {
      title: '更新时间',
      dataIndex: 'updateTime',
      width: 160,
    },
    {
      title: '操作',
      dataIndex: 'action',
      width: 140,
      fixed: 'right',
    },
  ];

  const queryFormDefault = {
    tenantId: 1,
    categoryId: undefined,
    searchWord: '',
    publishStatus: undefined,
    shelvesFlag: undefined,
    pageNum: 1,
    pageSize: PAGE_SIZE,
    searchCount: true,
  };
  const queryForm = reactive<any>({ ...queryFormDefault });
  const router = useRouter();
  const tableLoading = ref(false);
  const tableData = ref([]);
  const total = ref(0);
  const categoryTree = ref([]);

  onMounted(async () => {
    await queryCategoryTree();
    await queryPage();
  });

  async function queryCategoryTree() {
    try {
      const result = await shopCategoryApi.queryTree({ tenantId: 1, disabledFlag: false });
      categoryTree.value = result.data || [];
    } catch (error) {
      smartSentry.captureError(error);
    }
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
      const result = await shopProductApi.queryPage(queryForm);
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
    productId: undefined,
    tenantId: 1,
    categoryId: undefined,
    productName: '',
    productCode: '',
    subTitle: '',
    mainImage: [],
    detailImages: [],
    salePriceCent: 0,
    currency: 'USD',
    publishStatus: 1,
    shelvesFlag: false,
    sort: 0,
    seoTitle: '',
    seoDescription: '',
    productDetail: '',
    remark: '',
  };
  const form = reactive<any>({ ...formDefault });
  const rules = {
    categoryId: [{ required: true, message: '请选择商品类目' }],
    productName: [{ required: true, message: '请输入商品名称' }],
    salePriceCent: [{ required: true, message: '请输入价格' }],
    currency: [{ required: true, message: '请选择币种' }],
    publishStatus: [{ required: true, message: '请选择发布状态' }],
  };

  function showForm(productId?, rowData?) {
    Object.assign(form, formDefault);
    if (rowData) {
      Object.assign(form, rowData);
    }
    if (productId) {
      loadDetail(productId);
    }
    formVisible.value = true;
  }

  function showSku(record) {
    router.push({
      path: '/shop/product/sku',
      query: {
        productId: record.productId,
      },
    });
  }

  async function loadDetail(productId) {
    SmartLoading.show();
    try {
      const result = await shopProductApi.get(productId);
      Object.assign(form, result.data || {});
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function closeForm() {
    formVisible.value = false;
    Object.assign(form, formDefault);
  }

  function submit() {
    formRef.value
      .validate()
      .then(async () => {
        SmartLoading.show();
        try {
          if (form.productId) {
            await shopProductApi.update(form);
          } else {
            await shopProductApi.add(form);
          }
          message.success(`${form.productId ? '修改' : '添加'}成功`);
          closeForm();
          await queryPage();
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

  async function updateShelves(record, checked) {
    SmartLoading.show();
    try {
      await shopProductApi.updateShelves({
        productId: record.productId,
        shelvesFlag: checked,
      });
      message.success('状态更新成功');
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
      content: `确定要删除「${record.productName}」吗？`,
      okText: '确定',
      okType: 'danger',
      cancelText: '取消',
      async onOk() {
        await deleteProduct(record.productId);
      },
    });
  }

  async function deleteProduct(productId) {
    SmartLoading.show();
    try {
      await shopProductApi.delete(productId);
      message.success('删除成功');
      await queryPage();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function mainImageChange(fileList) {
    form.mainImage = fileList;
  }

  function detailImagesChange(fileList) {
    form.detailImages = fileList;
  }
</script>
