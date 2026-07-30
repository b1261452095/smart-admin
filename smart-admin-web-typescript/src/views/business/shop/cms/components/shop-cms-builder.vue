<template>
  <section class="cms-builder" v-privilege="'shop:cms:query'">
    <header class="cms-builder__toolbar">
      <div>
        <p class="cms-builder__eyebrow">店铺装修</p>
        <h2>首页区块</h2>
        <p>从左侧添加区块，在中间拖拽排序，右侧编辑当前区块。</p>
      </div>
      <div class="cms-builder__toolbar-actions">
        <a-tooltip title="重新加载">
          <a-button :loading="loading" @click="queryList">
            <template #icon><ReloadOutlined /></template>
          </a-button>
        </a-tooltip>
        <a-button :href="storefrontUrl" target="_blank">
          <template #icon><DesktopOutlined /></template>
          预览首页
        </a-button>
        <a-button type="primary" :disabled="!sortDirty || !blockList.length" @click="saveSort" v-privilege="'shop:cms:update'">
          <template #icon><SaveOutlined /></template>
          保存顺序
        </a-button>
      </div>
    </header>

    <a-alert
      v-if="sortDirty"
      class="cms-builder__notice"
      type="warning"
      show-icon
      message="区块顺序有未保存的修改"
    />

    <div class="cms-builder__workspace">
      <aside class="cms-panel cms-palette">
        <div class="cms-panel__head">
          <div>
            <span>区块库</span>
            <small>{{ blockTypeOptions.length }} 种</small>
          </div>
        </div>
        <div class="cms-palette__list">
          <button
            v-for="item in blockTypeOptions"
            :key="item.value"
            class="cms-palette__item"
            type="button"
            @click="createBlock(item.value)"
            v-privilege="'shop:cms:add'"
          >
            <component :is="item.icon" />
            <span>
              <strong>{{ item.label }}</strong>
              <small>{{ item.description }}</small>
            </span>
            <PlusOutlined />
          </button>
        </div>
      </aside>

      <main class="cms-panel cms-canvas">
        <div class="cms-panel__head">
          <div>
            <span>页面结构</span>
            <small>{{ blockList.length }} 个区块</small>
          </div>
          <span class="cms-canvas__status">{{ sortDirty ? '待保存' : '已同步' }}</span>
        </div>

        <a-spin :spinning="loading">
          <div v-if="!blockList.length" class="cms-canvas__empty">
            <AppstoreAddOutlined />
            <strong>还没有首页区块</strong>
            <span>从左侧区块库添加第一个区块。</span>
          </div>

          <div v-else ref="canvasRef" class="cms-canvas__list">
            <article
              v-for="(block, index) in blockList"
              :key="block.blockId"
              class="cms-block"
              :class="{ 'cms-block--selected': selectedBlockId === block.blockId, 'cms-block--disabled': block.disabledFlag }"
              tabindex="0"
              @click="selectBlock(block)"
              @keydown.enter="selectBlock(block)"
            >
              <button class="cms-block__handle" type="button" title="拖拽排序" @click.stop>
                <HolderOutlined />
              </button>
              <span class="cms-block__index">{{ String(index + 1).padStart(2, '0') }}</span>
              <div class="cms-block__media">
                <img v-if="getImageUrl(block.image)" :src="getImageUrl(block.image)" :alt="block.blockTitle || block.blockName" />
                <component :is="blockTypeMap[block.blockType]?.icon || AppstoreOutlined" v-else />
              </div>
              <div class="cms-block__copy">
                <div>
                  <a-tag :color="block.disabledFlag ? 'default' : 'blue'">{{ blockTypeMap[block.blockType]?.label || '未知区块' }}</a-tag>
                  <span v-if="block.disabledFlag" class="cms-block__muted">已隐藏</span>
                </div>
                <strong>{{ block.blockTitle || block.blockName }}</strong>
                <small>{{ block.blockSubTitle || block.productName || block.blockName }}</small>
              </div>
              <div class="cms-block__actions" @click.stop>
                <a-switch
                  size="small"
                  :checked="!block.disabledFlag"
                  :loading="statusLoadingId === block.blockId"
                  @change="updateDisabled(block, $event)"
                  v-privilege="'shop:cms:update'"
                />
                <a-tooltip title="复制区块">
                  <a-button type="text" size="small" @click="duplicateBlock(block)" v-privilege="'shop:cms:add'">
                    <template #icon><CopyOutlined /></template>
                  </a-button>
                </a-tooltip>
                <a-tooltip title="删除区块">
                  <a-button type="text" size="small" danger @click="confirmDelete(block)" v-privilege="'shop:cms:delete'">
                    <template #icon><DeleteOutlined /></template>
                  </a-button>
                </a-tooltip>
              </div>
            </article>
          </div>
        </a-spin>
      </main>

      <aside class="cms-panel cms-inspector">
        <div class="cms-panel__head">
          <div>
            <span>{{ form.blockId ? '区块设置' : isCreating ? '新增区块' : '属性面板' }}</span>
            <small v-if="form.blockId">ID {{ form.blockId }}</small>
          </div>
        </div>

        <div v-if="!form.blockId && !isCreating" class="cms-inspector__empty">
          <EditOutlined />
          <strong>选择一个区块</strong>
          <span>点击中间的区块后在这里编辑内容。</span>
        </div>

        <a-form
          v-else
          ref="formRef"
          class="cms-inspector__form"
          layout="vertical"
          :model="form"
          :rules="rules"
          @finish="submitBlock"
        >
          <a-form-item label="区块类型" name="blockType">
            <a-select v-model:value="form.blockType" :disabled="Boolean(form.blockId)" @change="onBlockTypeChange">
              <a-select-option v-for="item in blockTypeOptions" :key="item.value" :value="item.value">
                {{ item.label }}
              </a-select-option>
            </a-select>
          </a-form-item>

          <a-form-item label="内部名称" name="blockName">
            <a-input v-model:value="form.blockName" placeholder="仅后台识别，例如：首页主视觉" />
          </a-form-item>

          <a-form-item label="展示标题" name="blockTitle">
            <a-input v-model:value="form.blockTitle" placeholder="展示给顾客的标题" />
          </a-form-item>

          <a-form-item v-if="form.blockType !== BLOCK_TYPE_ANNOUNCEMENT" label="展示副标题" name="blockSubTitle">
            <a-textarea v-model:value="form.blockSubTitle" :rows="3" placeholder="可选，保持简短" />
          </a-form-item>

          <a-form-item v-if="form.blockType === BLOCK_TYPE_RECOMMEND_PRODUCT" label="推荐商品" name="productId">
            <a-select
              v-model:value="form.productId"
              show-search
              allow-clear
              :filter-option="filterProductOption"
              placeholder="选择商品"
            >
              <a-select-option
                v-for="item in productOptions"
                :key="item.productId"
                :value="item.productId"
                :label="item.productName"
              >
                {{ item.productName }}
              </a-select-option>
            </a-select>
          </a-form-item>

          <a-form-item
            v-if="[BLOCK_TYPE_BANNER, BLOCK_TYPE_NAVIGATION, BLOCK_TYPE_RECOMMEND_PRODUCT, BLOCK_TYPE_IMAGE_TEXT, BLOCK_TYPE_FULL_IMAGE, BLOCK_TYPE_VIDEO].includes(form.blockType)"
            label="图片"
            name="image"
          >
            <Upload
              accept=".jpg,.jpeg,.png,.webp"
              :maxUploadSize="1"
              :maxSize="8"
              buttonText="上传图片"
              :default-file-list="form.image"
              @change="imageChange"
            />
          </a-form-item>

          <a-form-item v-if="form.blockType !== BLOCK_TYPE_PRODUCT_GRID" label="跳转链接" name="linkUrl">
            <a-input v-model:value="form.linkUrl" placeholder="/collections/new-in" />
          </a-form-item>

          <template v-if="form.blockType === BLOCK_TYPE_BANNER">
            <a-form-item label="主视觉高度">
              <a-segmented v-model:value="configForm.height" block :options="heightOptions" />
            </a-form-item>
            <a-form-item label="文字位置">
              <a-select v-model:value="configForm.textPosition">
                <a-select-option value="left">左侧</a-select-option>
                <a-select-option value="center">居中</a-select-option>
                <a-select-option value="right">右侧</a-select-option>
              </a-select>
            </a-form-item>
            <a-form-item label="按钮文字">
              <a-input v-model:value="configForm.buttonText" placeholder="探索系列" />
            </a-form-item>
          </template>

          <template v-if="form.blockType === BLOCK_TYPE_NAVIGATION">
            <a-form-item label="展示方式">
              <a-segmented v-model:value="configForm.layout" block :options="navigationLayoutOptions" />
            </a-form-item>
          </template>

          <template v-if="form.blockType === BLOCK_TYPE_PRODUCT_GRID">
            <a-form-item label="分类标识">
              <a-input v-model:value="configForm.categorySlug" placeholder="留空表示全部商品" />
            </a-form-item>
            <div class="cms-inspector__two-cols">
              <a-form-item label="商品数量">
                <a-input-number v-model:value="configForm.limit" :min="2" :max="16" />
              </a-form-item>
              <a-form-item label="桌面列数">
                <a-input-number v-model:value="configForm.columns" :min="2" :max="5" />
              </a-form-item>
            </div>
            <a-form-item label="查看全部链接">
              <a-input v-model:value="configForm.collectionUrl" placeholder="/search" />
            </a-form-item>
          </template>

          <template v-if="form.blockType === BLOCK_TYPE_IMAGE_TEXT">
            <a-form-item label="图片位置">
              <a-segmented v-model:value="configForm.imagePosition" block :options="imagePositionOptions" />
            </a-form-item>
            <a-form-item label="按钮文字">
              <a-input v-model:value="configForm.buttonText" placeholder="了解更多" />
            </a-form-item>
          </template>

          <template v-if="form.blockType === BLOCK_TYPE_FULL_IMAGE">
            <a-form-item label="图片高度">
              <a-segmented v-model:value="configForm.height" block :options="heightOptions" />
            </a-form-item>
            <a-form-item label="按钮文字">
              <a-input v-model:value="configForm.buttonText" placeholder="探索系列" />
            </a-form-item>
          </template>

          <template v-if="form.blockType === BLOCK_TYPE_ANNOUNCEMENT">
            <a-form-item label="公告样式">
              <a-segmented v-model:value="configForm.theme" block :options="announcementThemeOptions" />
            </a-form-item>
          </template>

          <template v-if="form.blockType === BLOCK_TYPE_VIDEO">
            <a-form-item label="视频地址">
              <a-input v-model:value="configForm.videoUrl" placeholder="https://..." />
            </a-form-item>
            <a-form-item label="封面地址">
              <a-input v-model:value="configForm.poster" placeholder="https://..." />
            </a-form-item>
            <a-form-item>
              <a-checkbox v-model:checked="configForm.autoplay">静音自动播放</a-checkbox>
            </a-form-item>
          </template>

          <a-form-item label="状态">
            <a-switch v-model:checked="form.enabledFlag" checked-children="展示" un-checked-children="隐藏" />
          </a-form-item>

          <div class="cms-inspector__actions">
            <a-button @click="cancelEdit">取消</a-button>
            <a-button type="primary" html-type="submit" :loading="submitLoading" v-privilege="form.blockId ? 'shop:cms:update' : 'shop:cms:add'">
              <template #icon><SaveOutlined /></template>
              {{ form.blockId ? '保存区块' : '添加区块' }}
            </a-button>
          </div>
        </a-form>
      </aside>
    </div>
  </section>
</template>

<script setup lang="ts">
  import Sortable, { type SortableEvent } from 'sortablejs';
  import { computed, markRaw, nextTick, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
  import { message, Modal } from 'ant-design-vue';
  import {
    AppstoreAddOutlined,
    AppstoreOutlined,
    CopyOutlined,
    DeleteOutlined,
    DesktopOutlined,
    EditOutlined,
    ExpandOutlined,
    HolderOutlined,
    LayoutOutlined,
    NotificationOutlined,
    OrderedListOutlined,
    PictureOutlined,
    PlaySquareOutlined,
    PlusOutlined,
    ReloadOutlined,
    SaveOutlined,
    ShoppingOutlined,
  } from '@ant-design/icons-vue';
  import Upload from '/@/components/support/file-upload/index.vue';
  import { shopCmsApi } from '/@/api/business/shop/shop-cms-api';
  import { shopProductApi } from '/@/api/business/shop/shop-product-api';
  import { smartSentry } from '/@/lib/smart-sentry';

  const BLOCK_TYPE_BANNER = 1;
  const BLOCK_TYPE_NAVIGATION = 2;
  const BLOCK_TYPE_RECOMMEND_PRODUCT = 3;
  const BLOCK_TYPE_PRODUCT_GRID = 4;
  const BLOCK_TYPE_IMAGE_TEXT = 5;
  const BLOCK_TYPE_FULL_IMAGE = 6;
  const BLOCK_TYPE_ANNOUNCEMENT = 7;
  const BLOCK_TYPE_VIDEO = 8;

  type UploadFile = {
    fileUrl?: string;
    url?: string;
  };

  type CmsBlockRow = {
    blockId: number;
    blockType: number;
    blockName: string;
    blockTitle?: string;
    blockSubTitle?: string;
    image?: UploadFile[];
    linkUrl?: string;
    productId?: number;
    productName?: string;
    configJson?: string;
    sort?: number;
    disabledFlag?: boolean;
    version?: number;
    [key: string]: unknown;
  };

  const blockTypeOptions = [
    { value: BLOCK_TYPE_BANNER, label: '主视觉 Banner', description: '首页首屏大图与主标题', icon: markRaw(PictureOutlined) },
    { value: BLOCK_TYPE_NAVIGATION, label: '分类入口', description: '图片化分类或系列入口', icon: markRaw(AppstoreOutlined) },
    { value: BLOCK_TYPE_RECOMMEND_PRODUCT, label: '单品推荐', description: '重点展示一件商品', icon: markRaw(ShoppingOutlined) },
    { value: BLOCK_TYPE_PRODUCT_GRID, label: '商品列表', description: '新品或分类商品陈列', icon: markRaw(OrderedListOutlined) },
    { value: BLOCK_TYPE_IMAGE_TEXT, label: '图文双栏', description: '品牌内容与系列故事', icon: markRaw(LayoutOutlined) },
    { value: BLOCK_TYPE_FULL_IMAGE, label: '全幅图片', description: '沉浸式系列视觉', icon: markRaw(ExpandOutlined) },
    { value: BLOCK_TYPE_ANNOUNCEMENT, label: '公告栏', description: '顶部活动或服务提示', icon: markRaw(NotificationOutlined) },
    { value: BLOCK_TYPE_VIDEO, label: '视频区块', description: '短片与动态系列内容', icon: markRaw(PlaySquareOutlined) },
  ];
  const blockTypeMap = blockTypeOptions.reduce<Record<number, (typeof blockTypeOptions)[number]>>((map, item) => {
    map[item.value] = item;
    return map;
  }, {});

  const heightOptions = [
    { label: '紧凑', value: 'compact' },
    { label: '标准', value: 'tall' },
    { label: '满屏', value: 'full' },
  ];
  const navigationLayoutOptions = [
    { label: '网格', value: 'grid' },
    { label: '横向', value: 'rail' },
  ];
  const imagePositionOptions = [
    { label: '图片在左', value: 'left' },
    { label: '图片在右', value: 'right' },
  ];
  const announcementThemeOptions = [
    { label: '浅色', value: 'light' },
    { label: '深色', value: 'dark' },
  ];

  const storefrontUrl = computed(() => import.meta.env.VITE_STOREFRONT_URL || 'http://localhost:3100');
  const loading = ref(false);
  const submitLoading = ref(false);
  const statusLoadingId = ref<number>();
  const sortDirty = ref(false);
  const blockList = ref<CmsBlockRow[]>([]);
  const productOptions = ref<Array<{ productId: number; productName: string }>>([]);
  const selectedBlockId = ref<number>();
  const isCreating = ref(false);
  const canvasRef = ref<HTMLElement>();
  const formRef = ref();
  let sortable: Sortable | undefined;

  const form = reactive<any>(createForm());
  const configForm = reactive<any>({});
  const rules = {
    blockType: [{ required: true, message: '请选择区块类型' }],
    blockName: [{ required: true, message: '请输入内部名称' }],
    productId: [{ required: true, message: '请选择推荐商品' }],
  };

  onMounted(async () => {
    await Promise.all([queryList(), queryProductOptions()]);
  });

  onBeforeUnmount(() => {
    sortable?.destroy();
  });

  function createForm(blockType = BLOCK_TYPE_BANNER) {
    return {
      blockId: undefined,
      tenantId: 1,
      blockType,
      blockName: '',
      blockTitle: '',
      blockSubTitle: '',
      image: [],
      linkUrl: '',
      productId: undefined,
      configJson: '',
      sort: 0,
      enabledFlag: true,
      version: undefined,
    };
  }

  function defaultConfig(blockType: number) {
    const configMap: Record<number, Record<string, unknown>> = {
      [BLOCK_TYPE_BANNER]: { height: 'tall', textPosition: 'left', buttonText: '探索系列' },
      [BLOCK_TYPE_NAVIGATION]: { layout: 'grid' },
      [BLOCK_TYPE_RECOMMEND_PRODUCT]: { imagePosition: 'left', buttonText: '查看商品' },
      [BLOCK_TYPE_PRODUCT_GRID]: { categorySlug: '', limit: 8, columns: 4, collectionUrl: '/search' },
      [BLOCK_TYPE_IMAGE_TEXT]: { imagePosition: 'left', buttonText: '了解更多' },
      [BLOCK_TYPE_FULL_IMAGE]: { height: 'tall', buttonText: '探索系列' },
      [BLOCK_TYPE_ANNOUNCEMENT]: { theme: 'dark' },
      [BLOCK_TYPE_VIDEO]: { videoUrl: '', poster: '', autoplay: false },
    };
    return configMap[blockType] || {};
  }

  function setConfig(nextConfig: Record<string, unknown>) {
    Object.keys(configForm).forEach((key) => delete configForm[key]);
    Object.assign(configForm, nextConfig);
  }

  async function queryList() {
    loading.value = true;
    try {
      const result = await shopCmsApi.queryList({
        tenantId: 1,
        pageNum: 1,
        pageSize: 500,
        searchCount: false,
      });
      blockList.value = result.data || [];
      sortDirty.value = false;
      await nextTick();
      initSortable();
      if (selectedBlockId.value) {
        const selected = blockList.value.find((item) => item.blockId === selectedBlockId.value);
        if (selected) {
          selectBlock(selected);
        }
      }
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      loading.value = false;
    }
  }

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

  function initSortable() {
    sortable?.destroy();
    if (!canvasRef.value) {
      return;
    }
    sortable = Sortable.create(canvasRef.value, {
      animation: 220,
      handle: '.cms-block__handle',
      ghostClass: 'cms-block--ghost',
      chosenClass: 'cms-block--chosen',
      onEnd({ oldIndex, newIndex }: SortableEvent) {
        if (oldIndex === undefined || newIndex === undefined || oldIndex === newIndex) {
          return;
        }
        const movedBlock = blockList.value.splice(oldIndex, 1)[0];
        blockList.value.splice(newIndex, 0, movedBlock);
        blockList.value = [...blockList.value];
        sortDirty.value = true;
      },
    });
  }

  async function saveSort() {
    if (!blockList.value.length) {
      return;
    }
    loading.value = true;
    try {
      await shopCmsApi.updateSort({
        tenantId: 1,
        blockList: blockList.value.map((block, index) => ({
          blockId: block.blockId,
          sort: (index + 1) * 10,
          version: block.version,
        })),
      });
      message.success('首页区块顺序已保存');
      await queryList();
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      loading.value = false;
    }
  }

  function createBlock(blockType: number) {
    selectedBlockId.value = undefined;
    isCreating.value = true;
    Object.assign(form, createForm(blockType), {
      sort: (blockList.value.length + 1) * 10,
      blockName: blockTypeMap[blockType]?.label || '首页区块',
    });
    setConfig(defaultConfig(blockType));
    nextTick(() => formRef.value?.clearValidate());
  }

  function selectBlock(block: CmsBlockRow) {
    selectedBlockId.value = block.blockId;
    isCreating.value = false;
    Object.assign(form, createForm(block.blockType), block, {
      image: block.image || [],
      enabledFlag: !block.disabledFlag,
    });
    let savedConfig: Record<string, unknown> = {};
    try {
      savedConfig = block.configJson ? JSON.parse(block.configJson) : {};
    } catch {
      message.warning('当前区块的扩展配置无法解析，请重新保存');
    }
    setConfig({ ...defaultConfig(block.blockType), ...savedConfig });
    nextTick(() => formRef.value?.clearValidate());
  }

  function duplicateBlock(block: CmsBlockRow) {
    selectBlock(block);
    form.blockId = undefined;
    form.version = undefined;
    form.blockName = `${block.blockName} 副本`;
    form.sort = (blockList.value.length + 1) * 10;
    selectedBlockId.value = undefined;
    isCreating.value = true;
    message.info('已创建副本草稿，保存后加入页面');
  }

  function cancelEdit() {
    isCreating.value = false;
    selectedBlockId.value = undefined;
    Object.assign(form, createForm());
    setConfig({});
  }

  function onBlockTypeChange(blockType: number) {
    form.productId = undefined;
    setConfig(defaultConfig(blockType));
  }

  async function submitBlock() {
    submitLoading.value = true;
    try {
      const submitForm = {
        ...form,
        disabledFlag: !form.enabledFlag,
        configJson: JSON.stringify(configForm),
      };
      delete submitForm.enabledFlag;
      if (form.blockId) {
        await shopCmsApi.update(submitForm);
      } else {
        await shopCmsApi.add(submitForm);
      }
      message.success(form.blockId ? '区块已保存' : '区块已添加');
      const savedBlockId = form.blockId;
      const savedName = form.blockName;
      await queryList();
      const savedBlock = savedBlockId
        ? blockList.value.find((item) => item.blockId === savedBlockId)
        : [...blockList.value].reverse().find((item) => item.blockName === savedName);
      if (savedBlock) {
        selectBlock(savedBlock);
      } else {
        cancelEdit();
      }
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      submitLoading.value = false;
    }
  }

  async function updateDisabled(block: CmsBlockRow, checked: boolean) {
    statusLoadingId.value = block.blockId;
    try {
      await shopCmsApi.updateDisabled({
        blockId: block.blockId,
        disabledFlag: !checked,
      });
      block.disabledFlag = !checked;
      block.version = Number(block.version || 0) + 1;
      if (selectedBlockId.value === block.blockId) {
        form.enabledFlag = checked;
        form.version = block.version;
      }
    } catch (error) {
      smartSentry.captureError(error);
    } finally {
      statusLoadingId.value = undefined;
    }
  }

  function confirmDelete(block: CmsBlockRow) {
    Modal.confirm({
      title: '删除区块',
      content: `确定删除「${block.blockTitle || block.blockName}」吗？`,
      okText: '删除',
      okType: 'danger',
      cancelText: '取消',
      async onOk() {
        try {
          await shopCmsApi.delete(block.blockId);
          message.success('区块已删除');
          if (selectedBlockId.value === block.blockId) {
            cancelEdit();
          }
          await queryList();
        } catch (error) {
          smartSentry.captureError(error);
        }
      },
    });
  }

  function filterProductOption(input: string, option: { label?: string }) {
    return String(option.label || '')
      .toLowerCase()
      .includes(String(input || '').toLowerCase());
  }

  function imageChange(fileList: UploadFile[]) {
    form.image = fileList;
  }

  function getImageUrl(imageList?: UploadFile[]) {
    if (!imageList || imageList.length === 0) {
      return '';
    }
    return imageList[0].fileUrl || imageList[0].url || '';
  }
</script>

<style scoped lang="less">
  .cms-builder {
    --cms-ink: #182230;
    --cms-muted: #667085;
    --cms-line: #e4e7ec;
    --cms-surface: #ffffff;
    --cms-canvas: #f4f6f8;
    --cms-accent: #1677ff;
    --cms-accent-soft: #eaf3ff;
    display: grid;
    gap: 12px;
    min-width: 0;
  }

  .cms-builder__toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 24px;
    padding: 18px 20px;
    border: 1px solid var(--cms-line);
    border-radius: 6px;
    background: var(--cms-surface);
  }

  .cms-builder__toolbar h2 {
    margin: 0;
    color: var(--cms-ink);
    font-size: 20px;
    line-height: 1.3;
  }

  .cms-builder__toolbar p {
    margin: 4px 0 0;
    color: var(--cms-muted);
  }

  .cms-builder__toolbar .cms-builder__eyebrow {
    margin: 0 0 2px;
    color: var(--cms-accent);
    font-size: 12px;
    font-weight: 700;
  }

  .cms-builder__toolbar-actions {
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 0 0 auto;
  }

  .cms-builder__notice {
    margin: 0;
  }

  .cms-builder__workspace {
    display: grid;
    grid-template-columns: 220px minmax(420px, 1fr) 360px;
    gap: 12px;
    min-width: 0;
    min-height: 680px;
  }

  .cms-panel {
    min-width: 0;
    overflow: hidden;
    border: 1px solid var(--cms-line);
    border-radius: 6px;
    background: var(--cms-surface);
  }

  .cms-panel__head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    min-height: 52px;
    padding: 0 16px;
    border-bottom: 1px solid var(--cms-line);
  }

  .cms-panel__head > div {
    display: flex;
    align-items: baseline;
    gap: 8px;
  }

  .cms-panel__head span {
    color: var(--cms-ink);
    font-weight: 700;
  }

  .cms-panel__head small {
    color: var(--cms-muted);
  }

  .cms-palette__list {
    display: grid;
    gap: 8px;
    padding: 12px;
  }

  .cms-palette__item {
    display: grid;
    grid-template-columns: 24px minmax(0, 1fr) 16px;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 11px 10px;
    border: 1px solid var(--cms-line);
    border-radius: 4px;
    background: var(--cms-surface);
    color: var(--cms-muted);
    text-align: left;
    cursor: pointer;
    transition: border-color 120ms ease, background 120ms ease, color 120ms ease;
  }

  .cms-palette__item:hover,
  .cms-palette__item:focus-visible {
    border-color: var(--cms-accent);
    background: var(--cms-accent-soft);
    color: var(--cms-accent);
    outline: none;
  }

  .cms-palette__item span {
    display: grid;
    min-width: 0;
  }

  .cms-palette__item strong {
    color: var(--cms-ink);
    font-size: 13px;
  }

  .cms-palette__item small {
    overflow: hidden;
    color: var(--cms-muted);
    font-size: 11px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .cms-canvas {
    background: var(--cms-canvas);
  }

  .cms-canvas .cms-panel__head {
    background: var(--cms-surface);
  }

  .cms-canvas__status {
    color: var(--cms-muted) !important;
    font-size: 12px;
    font-weight: 400 !important;
  }

  .cms-canvas__empty,
  .cms-inspector__empty {
    display: grid;
    align-content: center;
    justify-items: center;
    gap: 8px;
    min-height: 360px;
    padding: 32px;
    color: var(--cms-muted);
    text-align: center;
  }

  .cms-canvas__empty > .anticon,
  .cms-inspector__empty > .anticon {
    font-size: 30px;
  }

  .cms-canvas__empty strong,
  .cms-inspector__empty strong {
    color: var(--cms-ink);
  }

  .cms-canvas__list {
    display: grid;
    gap: 10px;
    padding: 14px;
  }

  .cms-block {
    display: grid;
    grid-template-columns: 32px 28px 92px minmax(0, 1fr) auto;
    align-items: center;
    gap: 10px;
    min-width: 0;
    min-height: 92px;
    padding: 10px 12px 10px 6px;
    border: 1px solid var(--cms-line);
    border-radius: 5px;
    background: var(--cms-surface);
    box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
    cursor: pointer;
    transition: border-color 120ms ease, box-shadow 120ms ease;
  }

  .cms-block:hover,
  .cms-block:focus-visible,
  .cms-block--selected {
    border-color: var(--cms-accent);
    outline: none;
    box-shadow: 0 0 0 2px rgba(22, 119, 255, 0.1);
  }

  .cms-block--disabled {
    opacity: 0.62;
  }

  .cms-block--ghost {
    border-style: dashed;
    background: var(--cms-accent-soft);
    opacity: 0.72;
  }

  .cms-block--chosen {
    box-shadow: 0 8px 24px rgba(16, 24, 40, 0.14);
  }

  .cms-block__handle {
    display: inline-grid;
    width: 32px;
    height: 40px;
    padding: 0;
    border: 0;
    background: transparent;
    color: #98a2b3;
    cursor: grab;
    place-items: center;
  }

  .cms-block__handle:active {
    cursor: grabbing;
  }

  .cms-block__index {
    color: #98a2b3;
    font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
    font-size: 11px;
  }

  .cms-block__media {
    display: grid;
    width: 92px;
    height: 64px;
    overflow: hidden;
    border-radius: 3px;
    background: #eef1f4;
    color: #98a2b3;
    font-size: 24px;
    place-items: center;
  }

  .cms-block__media img {
    width: 100%;
    height: 100%;
    object-fit: cover;
  }

  .cms-block__copy {
    display: grid;
    gap: 4px;
    min-width: 0;
  }

  .cms-block__copy > div {
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .cms-block__copy strong,
  .cms-block__copy small {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .cms-block__copy strong {
    color: var(--cms-ink);
  }

  .cms-block__copy small,
  .cms-block__muted {
    color: var(--cms-muted);
    font-size: 12px;
  }

  .cms-block__actions {
    display: flex;
    align-items: center;
    gap: 2px;
  }

  .cms-inspector {
    overflow: visible;
  }

  .cms-inspector__form {
    max-height: calc(100vh - 230px);
    overflow-y: auto;
    padding: 16px;
  }

  .cms-inspector__two-cols {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }

  .cms-inspector__two-cols :deep(.ant-input-number) {
    width: 100%;
  }

  .cms-inspector__actions {
    position: sticky;
    bottom: 0;
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding-top: 14px;
    border-top: 1px solid var(--cms-line);
    background: var(--cms-surface);
  }

  @media (max-width: 1320px) {
    .cms-builder__workspace {
      grid-template-columns: 190px minmax(0, 1fr);
    }

    .cms-inspector {
      grid-column: 1 / -1;
    }

    .cms-inspector__form {
      max-height: none;
    }
  }

  @media (max-width: 860px) {
    .cms-builder__toolbar {
      align-items: flex-start;
      flex-direction: column;
    }

    .cms-builder__toolbar-actions {
      flex-wrap: wrap;
    }

    .cms-builder__workspace {
      grid-template-columns: minmax(0, 1fr);
    }

    .cms-inspector {
      grid-column: auto;
    }

    .cms-palette__list {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .cms-block {
      grid-template-columns: 28px 24px 72px minmax(0, 1fr);
    }

    .cms-block__media {
      width: 72px;
      height: 56px;
    }

    .cms-block__actions {
      grid-column: 4;
      justify-content: flex-start;
    }
  }
</style>
