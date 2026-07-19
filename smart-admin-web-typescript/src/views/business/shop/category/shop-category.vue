<template>
  <a-card size="small" :bordered="false" :hoverable="true">
    <a-row class="smart-table-btn-block">
      <div class="smart-table-operate-block">
        <a-button type="primary" @click="showForm()" v-privilege="'shop:category:add'">
          <template #icon>
            <PlusOutlined />
          </template>
          新建类目
        </a-button>
        <a-button @click="queryTree" v-privilege="'shop:category:query'">
          <template #icon>
            <ReloadOutlined />
          </template>
          刷新
        </a-button>
      </div>
    </a-row>

    <a-table
      size="small"
      :dataSource="tableData"
      :columns="columns"
      rowKey="categoryId"
      bordered
      :pagination="false"
      :loading="tableLoading"
      :scroll="{ x: 1000 }"
    >
      <template #bodyCell="{ record, column }">
        <template v-if="column.dataIndex === 'disabledFlag'">
          <a-switch
            :checked="!record.disabledFlag"
            checked-children="启用"
            un-checked-children="禁用"
            @change="(checked) => updateDisabled(record, checked)"
            v-privilege="'shop:category:updateDisabled'"
          />
        </template>
        <template v-if="column.dataIndex === 'action'">
          <div class="smart-table-operate">
            <a-button type="link" @click="showForm(record.categoryId)" v-privilege="'shop:category:add'">增加子类</a-button>
            <a-button type="link" @click="showForm(undefined, record)" v-privilege="'shop:category:update'">编辑</a-button>
            <a-button type="link" danger @click="confirmDelete(record)" v-privilege="'shop:category:delete'">删除</a-button>
          </div>
        </template>
      </template>
    </a-table>

    <a-modal
      :open="formVisible"
      :title="form.categoryId ? '编辑类目' : '新增类目'"
      :width="680"
      forceRender
      ok-text="确认"
      cancel-text="取消"
      @ok="submit"
      @cancel="closeForm"
    >
      <a-form ref="formRef" :model="form" :rules="rules" :label-col="{ span: 5 }" :wrapper-col="{ span: 16 }">
        <a-form-item label="父级类目" v-if="parentName">
          <a-input :value="parentName" disabled />
        </a-form-item>

        <a-form-item label="类目名称" name="categoryName">
          <a-input v-model:value="form.categoryName" placeholder="请输入类目名称" />
        </a-form-item>

        <a-form-item label="类目编码" name="categoryCode">
          <a-input v-model:value="form.categoryCode" placeholder="可选，用于后续对接搜索/导入" />
        </a-form-item>

        <a-form-item label="类目图片" name="categoryImage">
          <Upload
            accept=".jpg,.jpeg,.png,.gif"
            :maxUploadSize="1"
            :maxSize="5"
            buttonText="上传类目图"
            :default-file-list="form.categoryImage"
            @change="categoryImageChange"
          />
        </a-form-item>

        <a-form-item label="排序" name="sort">
          <a-input-number v-model:value="form.sort" style="width: 160px" :min="0" />
        </a-form-item>

        <a-form-item label="启用状态" name="disabledFlag">
          <a-switch :checked="!form.disabledFlag" checked-children="启用" un-checked-children="禁用" @change="enabledChange" />
        </a-form-item>

        <a-form-item label="备注" name="remark">
          <a-textarea v-model:value="form.remark" :rows="3" placeholder="内部备注，不展示给用户" />
        </a-form-item>
      </a-form>
    </a-modal>
  </a-card>
</template>

<script setup lang="ts">
  import { onMounted, reactive, ref } from 'vue';
  import { message, Modal } from 'ant-design-vue';
  import Upload from '/@/components/support/file-upload/index.vue';
  import { SmartLoading } from '/@/components/framework/smart-loading';
  import { smartSentry } from '/@/lib/smart-sentry';
  import { shopCategoryApi } from '/@/api/business/shop/shop-category-api';

  const columns = [
    {
      title: '类目名称',
      dataIndex: 'categoryName',
      minWidth: 180,
    },
    {
      title: '类目编码',
      dataIndex: 'categoryCode',
      width: 160,
    },
    {
      title: '排序',
      dataIndex: 'sort',
      width: 80,
    },
    {
      title: '状态',
      dataIndex: 'disabledFlag',
      width: 100,
    },
    {
      title: '更新时间',
      dataIndex: 'updateTime',
      width: 160,
    },
    {
      title: '操作',
      dataIndex: 'action',
      width: 220,
    },
  ];

  const tableLoading = ref(false);
  const tableData = ref([]);

  onMounted(() => {
    queryTree();
  });

  async function queryTree() {
    tableLoading.value = true;
    try {
      const result = await shopCategoryApi.queryTree({ tenantId: 1 });
      tableData.value = result.data || [];
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      tableLoading.value = false;
    }
  }

  const formRef = ref();
  const formVisible = ref(false);
  const parentName = ref('');

  const formDefault = {
    categoryId: undefined,
    tenantId: 1,
    parentId: undefined,
    categoryName: '',
    categoryCode: '',
    categoryImage: [],
    sort: 0,
    disabledFlag: false,
    remark: '',
  };
  const form = reactive<any>({ ...formDefault });

  const rules = {
    categoryName: [{ required: true, message: '请输入类目名称' }],
  };

  function showForm(parentId?, rowData?) {
    Object.assign(form, formDefault);
    parentName.value = '';
    if (parentId) {
      form.parentId = parentId;
      parentName.value = findCategoryName(tableData.value, parentId);
    }
    if (rowData) {
      Object.assign(form, rowData);
      parentName.value = rowData.parentId ? findCategoryName(tableData.value, rowData.parentId) : '';
    }
    formVisible.value = true;
  }

  function closeForm() {
    formVisible.value = false;
    Object.assign(form, formDefault);
    parentName.value = '';
  }

  function submit() {
    formRef.value
      .validate()
      .then(async () => {
        SmartLoading.show();
        try {
          if (form.categoryId) {
            await shopCategoryApi.update(form);
          } else {
            await shopCategoryApi.add(form);
          }
          message.success(`${form.categoryId ? '修改' : '添加'}成功`);
          closeForm();
          await queryTree();
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

  function confirmDelete(record) {
    Modal.confirm({
      title: '提示',
      content: `确定要删除「${record.categoryName}」吗？`,
      okText: '确定',
      okType: 'danger',
      cancelText: '取消',
      async onOk() {
        await deleteCategory(record.categoryId);
      },
    });
  }

  async function deleteCategory(categoryId) {
    SmartLoading.show();
    try {
      await shopCategoryApi.delete(categoryId);
      message.success('删除成功');
      await queryTree();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  async function updateDisabled(record, checked) {
    SmartLoading.show();
    try {
      await shopCategoryApi.updateDisabled({
        categoryId: record.categoryId,
        disabledFlag: !checked,
      });
      message.success('状态更新成功');
      await queryTree();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      SmartLoading.hide();
    }
  }

  function enabledChange(checked) {
    form.disabledFlag = !checked;
  }

  function categoryImageChange(fileList) {
    form.categoryImage = fileList;
  }

  function findCategoryName(list, categoryId) {
    for (const item of list || []) {
      if (item.categoryId === categoryId) {
        return item.categoryName;
      }
      const childName = findCategoryName(item.children, categoryId);
      if (childName) {
        return childName;
      }
    }
    return '';
  }
</script>
