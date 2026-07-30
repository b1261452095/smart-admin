package net.lab1024.sa.admin.module.business.shop.cms.service;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.cms.dao.ShopCmsBlockDao;
import net.lab1024.sa.admin.module.business.shop.cms.domain.entity.ShopCmsBlockEntity;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockAddForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockClientQueryForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockDisabledForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockQueryForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockSortForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockUpdateForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.vo.ShopCmsBlockVO;
import net.lab1024.sa.admin.module.business.shop.product.dao.ShopProductDao;
import net.lab1024.sa.admin.module.business.shop.product.domain.entity.ShopProductEntity;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.exception.BusinessException;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import net.lab1024.sa.base.common.util.SmartPageUtil;
import net.lab1024.sa.base.common.util.SmartStringUtil;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Shop CMS block service.
 */
@Service
public class ShopCmsBlockService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    private static final Integer BLOCK_TYPE_BANNER = 1;

    private static final Integer BLOCK_TYPE_NAVIGATION = 2;

    private static final Integer BLOCK_TYPE_RECOMMEND_PRODUCT = 3;

    private static final Integer BLOCK_TYPE_PRODUCT_GRID = 4;

    private static final Integer BLOCK_TYPE_IMAGE_TEXT = 5;

    private static final Integer BLOCK_TYPE_FULL_IMAGE = 6;

    private static final Integer BLOCK_TYPE_ANNOUNCEMENT = 7;

    private static final Integer BLOCK_TYPE_VIDEO = 8;

    private static final Set<Integer> BLOCK_TYPE_SET = Set.of(
            BLOCK_TYPE_BANNER,
            BLOCK_TYPE_NAVIGATION,
            BLOCK_TYPE_RECOMMEND_PRODUCT,
            BLOCK_TYPE_PRODUCT_GRID,
            BLOCK_TYPE_IMAGE_TEXT,
            BLOCK_TYPE_FULL_IMAGE,
            BLOCK_TYPE_ANNOUNCEMENT,
            BLOCK_TYPE_VIDEO
    );

    @Resource
    private ShopCmsBlockDao shopCmsBlockDao;

    @Resource
    private ShopProductDao shopProductDao;

    @Resource
    private ObjectMapper objectMapper;

    /**
     * Query CMS block page.
     */
    public ResponseDTO<PageResult<ShopCmsBlockVO>> queryPage(ShopCmsBlockQueryForm queryForm) {
        Long tenantId = queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId();
        LambdaQueryWrapper<ShopCmsBlockEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCmsBlockEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        if (queryForm.getBlockType() != null) {
            queryWrapper.eq(ShopCmsBlockEntity::getBlockType, queryForm.getBlockType());
        }
        if (queryForm.getDisabledFlag() != null) {
            queryWrapper.eq(ShopCmsBlockEntity::getDisabledFlag, queryForm.getDisabledFlag());
        }
        if (SmartStringUtil.isNotEmpty(queryForm.getSearchWord())) {
            queryWrapper.and(wrapper -> wrapper.like(ShopCmsBlockEntity::getBlockName, queryForm.getSearchWord())
                    .or()
                    .like(ShopCmsBlockEntity::getBlockTitle, queryForm.getSearchWord())
                    .or()
                    .like(ShopCmsBlockEntity::getProductName, queryForm.getSearchWord()));
        }
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getSort);
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getBlockId);

        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopCmsBlockEntity> resultPage = shopCmsBlockDao.selectPage((Page<ShopCmsBlockEntity>) page, queryWrapper);
        List<ShopCmsBlockVO> blockList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopCmsBlockVO.class);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, blockList));
    }

    /**
     * Query all CMS blocks in storefront page order.
     */
    public ResponseDTO<List<ShopCmsBlockVO>> queryList(ShopCmsBlockQueryForm queryForm) {
        Long tenantId = queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId();
        LambdaQueryWrapper<ShopCmsBlockEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCmsBlockEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        if (queryForm.getBlockType() != null) {
            queryWrapper.eq(ShopCmsBlockEntity::getBlockType, queryForm.getBlockType());
        }
        if (queryForm.getDisabledFlag() != null) {
            queryWrapper.eq(ShopCmsBlockEntity::getDisabledFlag, queryForm.getDisabledFlag());
        }
        if (SmartStringUtil.isNotEmpty(queryForm.getSearchWord())) {
            queryWrapper.and(wrapper -> wrapper.like(ShopCmsBlockEntity::getBlockName, queryForm.getSearchWord())
                    .or()
                    .like(ShopCmsBlockEntity::getBlockTitle, queryForm.getSearchWord())
                    .or()
                    .like(ShopCmsBlockEntity::getProductName, queryForm.getSearchWord()));
        }
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getSort);
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getBlockId);
        return ResponseDTO.ok(SmartBeanUtil.copyList(shopCmsBlockDao.selectList(queryWrapper), ShopCmsBlockVO.class));
    }

    /**
     * Get CMS block.
     */
    public ResponseDTO<ShopCmsBlockVO> get(Long blockId) {
        ShopCmsBlockEntity blockEntity = getValidBlock(blockId);
        if (blockEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }
        return ResponseDTO.ok(SmartBeanUtil.copy(blockEntity, ShopCmsBlockVO.class));
    }

    /**
     * Query enabled CMS blocks for C-side homepage.
     */
    public ResponseDTO<List<ShopCmsBlockVO>> queryClientList(ShopCmsBlockClientQueryForm queryForm) {
        Long tenantId = queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId();
        LambdaQueryWrapper<ShopCmsBlockEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCmsBlockEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.eq(ShopCmsBlockEntity::getDisabledFlag, Boolean.FALSE);
        if (queryForm.getBlockType() != null) {
            queryWrapper.eq(ShopCmsBlockEntity::getBlockType, queryForm.getBlockType());
        }
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getSort);
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getBlockId);
        return ResponseDTO.ok(SmartBeanUtil.copyList(shopCmsBlockDao.selectList(queryWrapper), ShopCmsBlockVO.class));
    }

    /**
     * Add CMS block.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> add(ShopCmsBlockAddForm addForm, RequestUser requestUser) {
        Long tenantId = addForm.getTenantId() == null ? DEFAULT_TENANT_ID : addForm.getTenantId();
        ResponseDTO<ShopCmsBlockEntity> buildResult = buildSaveEntity(null, tenantId, addForm);
        if (!buildResult.getOk()) {
            return ResponseDTO.error(buildResult);
        }

        ShopCmsBlockEntity blockEntity = buildResult.getData();
        blockEntity.setCreateUserId(requestUser.getUserId());
        blockEntity.setUpdateUserId(requestUser.getUserId());
        shopCmsBlockDao.insert(blockEntity);
        return ResponseDTO.ok();
    }

    /**
     * Update CMS block.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> update(ShopCmsBlockUpdateForm updateForm, RequestUser requestUser) {
        ShopCmsBlockEntity originEntity = getValidBlock(updateForm.getBlockId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }
        ResponseDTO<ShopCmsBlockEntity> buildResult = buildSaveEntity(updateForm.getBlockId(), originEntity.getTenantId(), updateForm);
        if (!buildResult.getOk()) {
            return ResponseDTO.error(buildResult);
        }

        ShopCmsBlockEntity blockEntity = buildResult.getData();
        LambdaUpdateWrapper<ShopCmsBlockEntity> updateWrapper = new LambdaUpdateWrapper<>();
        updateWrapper.set(ShopCmsBlockEntity::getBlockType, blockEntity.getBlockType());
        updateWrapper.set(ShopCmsBlockEntity::getBlockName, blockEntity.getBlockName());
        updateWrapper.set(ShopCmsBlockEntity::getBlockTitle, blockEntity.getBlockTitle());
        updateWrapper.set(ShopCmsBlockEntity::getBlockSubTitle, blockEntity.getBlockSubTitle());
        updateWrapper.set(ShopCmsBlockEntity::getImage, blockEntity.getImage());
        updateWrapper.set(ShopCmsBlockEntity::getLinkUrl, blockEntity.getLinkUrl());
        updateWrapper.set(ShopCmsBlockEntity::getProductId, blockEntity.getProductId());
        updateWrapper.set(ShopCmsBlockEntity::getProductName, blockEntity.getProductName());
        updateWrapper.set(ShopCmsBlockEntity::getConfigJson, blockEntity.getConfigJson());
        updateWrapper.set(ShopCmsBlockEntity::getSort, blockEntity.getSort());
        updateWrapper.set(ShopCmsBlockEntity::getDisabledFlag, blockEntity.getDisabledFlag());
        updateWrapper.set(ShopCmsBlockEntity::getUpdateUserId, requestUser.getUserId());
        updateWrapper.setSql("version = version + 1");
        updateWrapper.eq(ShopCmsBlockEntity::getBlockId, updateForm.getBlockId());
        updateWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        updateWrapper.eq(ShopCmsBlockEntity::getVersion, updateForm.getVersion());
        if (shopCmsBlockDao.update(null, updateWrapper) != 1) {
            return ResponseDTO.userErrorParam("页面内容已被其他人修改，请刷新后重试");
        }
        return ResponseDTO.ok();
    }

    /**
     * Update CMS block disabled flag.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateDisabled(ShopCmsBlockDisabledForm disabledForm, RequestUser requestUser) {
        ShopCmsBlockEntity originEntity = getValidBlock(disabledForm.getBlockId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        LambdaUpdateWrapper<ShopCmsBlockEntity> updateWrapper = new LambdaUpdateWrapper<>();
        updateWrapper.set(ShopCmsBlockEntity::getDisabledFlag, disabledForm.getDisabledFlag());
        updateWrapper.set(ShopCmsBlockEntity::getUpdateUserId, requestUser.getUserId());
        updateWrapper.setSql("version = version + 1");
        updateWrapper.eq(ShopCmsBlockEntity::getBlockId, disabledForm.getBlockId());
        updateWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        updateWrapper.eq(ShopCmsBlockEntity::getVersion, originEntity.getVersion());
        if (shopCmsBlockDao.update(null, updateWrapper) != 1) {
            return ResponseDTO.userErrorParam("页面内容已被其他人修改，请刷新后重试");
        }
        return ResponseDTO.ok();
    }

    /**
     * Update storefront block order with optimistic version checks.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateSort(ShopCmsBlockSortForm sortForm, RequestUser requestUser) {
        Long tenantId = sortForm.getTenantId() == null ? DEFAULT_TENANT_ID : sortForm.getTenantId();
        List<ShopCmsBlockSortForm.SortItem> sortItems = sortForm.getBlockList();
        Set<Long> blockIdSet = new HashSet<>();
        for (ShopCmsBlockSortForm.SortItem item : sortItems) {
            if (!blockIdSet.add(item.getBlockId())) {
                return ResponseDTO.userErrorParam("区块排序中存在重复数据");
            }
        }

        LambdaQueryWrapper<ShopCmsBlockEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCmsBlockEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.in(ShopCmsBlockEntity::getBlockId, blockIdSet);
        List<ShopCmsBlockEntity> blockList = shopCmsBlockDao.selectList(queryWrapper);
        if (blockList.size() != blockIdSet.size()) {
            return ResponseDTO.userErrorParam("部分区块不存在或已被删除，请刷新后重试");
        }

        Map<Long, ShopCmsBlockEntity> blockMap = new HashMap<>();
        for (ShopCmsBlockEntity blockEntity : blockList) {
            blockMap.put(blockEntity.getBlockId(), blockEntity);
        }
        for (ShopCmsBlockSortForm.SortItem item : sortItems) {
            ShopCmsBlockEntity originEntity = blockMap.get(item.getBlockId());
            if (!Objects.equals(originEntity.getVersion(), item.getVersion())) {
                return ResponseDTO.userErrorParam("页面内容已被其他人修改，请刷新后重新排序");
            }
        }

        for (ShopCmsBlockSortForm.SortItem item : sortItems) {
            LambdaUpdateWrapper<ShopCmsBlockEntity> updateWrapper = new LambdaUpdateWrapper<>();
            updateWrapper.set(ShopCmsBlockEntity::getSort, item.getSort());
            updateWrapper.set(ShopCmsBlockEntity::getUpdateUserId, requestUser.getUserId());
            updateWrapper.setSql("version = version + 1");
            updateWrapper.eq(ShopCmsBlockEntity::getBlockId, item.getBlockId());
            updateWrapper.eq(ShopCmsBlockEntity::getTenantId, tenantId);
            updateWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
            updateWrapper.eq(ShopCmsBlockEntity::getVersion, item.getVersion());
            if (shopCmsBlockDao.update(null, updateWrapper) != 1) {
                throw new BusinessException("保存排序失败，页面内容可能已被修改");
            }
        }
        return ResponseDTO.ok();
    }

    /**
     * Delete CMS block.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> delete(Long blockId, RequestUser requestUser) {
        ShopCmsBlockEntity originEntity = getValidBlock(blockId);
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopCmsBlockEntity deleteEntity = new ShopCmsBlockEntity();
        deleteEntity.setBlockId(blockId);
        deleteEntity.setDeletedFlag(Boolean.TRUE);
        deleteEntity.setUpdateUserId(requestUser.getUserId());
        shopCmsBlockDao.updateById(deleteEntity);
        return ResponseDTO.ok();
    }

    private ResponseDTO<ShopCmsBlockEntity> buildSaveEntity(Long blockId, Long tenantId, ShopCmsBlockAddForm blockForm) {
        if (!BLOCK_TYPE_SET.contains(blockForm.getBlockType())) {
            return ResponseDTO.userErrorParam("区块类型不正确");
        }

        ShopCmsBlockEntity blockEntity = SmartBeanUtil.copy(blockForm, ShopCmsBlockEntity.class);
        blockEntity.setBlockId(blockId);
        blockEntity.setTenantId(tenantId);
        blockEntity.setSort(blockEntity.getSort() == null ? 0 : blockEntity.getSort());
        blockEntity.setDisabledFlag(blockEntity.getDisabledFlag() == null ? Boolean.FALSE : blockEntity.getDisabledFlag());
        blockEntity.setDeletedFlag(Boolean.FALSE);
        normalize(blockEntity);

        ResponseDTO<String> configResult = validateConfigJson(blockEntity.getConfigJson());
        if (!configResult.getOk()) {
            return ResponseDTO.error(configResult);
        }

        if (Objects.equals(blockForm.getBlockType(), BLOCK_TYPE_RECOMMEND_PRODUCT)) {
            ResponseDTO<String> productResult = fillRecommendProduct(blockEntity, tenantId);
            if (!productResult.getOk()) {
                return ResponseDTO.error(productResult);
            }
        } else {
            blockEntity.setProductId(null);
            blockEntity.setProductName(null);
        }

        return ResponseDTO.ok(blockEntity);
    }

    private ResponseDTO<String> fillRecommendProduct(ShopCmsBlockEntity blockEntity, Long tenantId) {
        if (blockEntity.getProductId() == null) {
            return ResponseDTO.userErrorParam("推荐商品不能为空");
        }
        ShopProductEntity productEntity = shopProductDao.selectById(blockEntity.getProductId());
        if (productEntity == null || Boolean.TRUE.equals(productEntity.getDeletedFlag())) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "推荐商品不存在");
        }
        if (!Objects.equals(productEntity.getTenantId(), tenantId)) {
            return ResponseDTO.userErrorParam("推荐商品租户不一致");
        }
        blockEntity.setProductName(productEntity.getProductName());
        if (SmartStringUtil.isEmpty(blockEntity.getImage())) {
            blockEntity.setImage(productEntity.getMainImage());
        }
        return ResponseDTO.ok();
    }

    private ShopCmsBlockEntity getValidBlock(Long blockId) {
        if (blockId == null) {
            return null;
        }
        LambdaQueryWrapper<ShopCmsBlockEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCmsBlockEntity::getBlockId, blockId);
        queryWrapper.eq(ShopCmsBlockEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopCmsBlockDao.selectOne(queryWrapper);
    }

    private void normalize(ShopCmsBlockEntity blockEntity) {
        if (SmartStringUtil.isEmpty(blockEntity.getBlockTitle())) {
            blockEntity.setBlockTitle(null);
        }
        if (SmartStringUtil.isEmpty(blockEntity.getBlockSubTitle())) {
            blockEntity.setBlockSubTitle(null);
        }
        if (SmartStringUtil.isEmpty(blockEntity.getImage())) {
            blockEntity.setImage(null);
        }
        if (SmartStringUtil.isEmpty(blockEntity.getLinkUrl())) {
            blockEntity.setLinkUrl(null);
        }
        if (SmartStringUtil.isEmpty(blockEntity.getConfigJson())) {
            blockEntity.setConfigJson(null);
        }
    }

    private ResponseDTO<String> validateConfigJson(String configJson) {
        if (SmartStringUtil.isEmpty(configJson)) {
            return ResponseDTO.ok();
        }
        try {
            objectMapper.readTree(configJson);
            return ResponseDTO.ok();
        } catch (JsonProcessingException e) {
            return ResponseDTO.userErrorParam("扩展配置不是有效的JSON");
        }
    }
}
