package net.lab1024.sa.admin.module.business.shop.cms.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.cms.dao.ShopCmsBlockDao;
import net.lab1024.sa.admin.module.business.shop.cms.domain.entity.ShopCmsBlockEntity;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockAddForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockClientQueryForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockDisabledForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockQueryForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockUpdateForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.vo.ShopCmsBlockVO;
import net.lab1024.sa.admin.module.business.shop.product.dao.ShopProductDao;
import net.lab1024.sa.admin.module.business.shop.product.domain.entity.ShopProductEntity;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import net.lab1024.sa.base.common.util.SmartPageUtil;
import net.lab1024.sa.base.common.util.SmartStringUtil;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
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

    private static final Set<Integer> BLOCK_TYPE_SET = Set.of(BLOCK_TYPE_BANNER, BLOCK_TYPE_NAVIGATION, BLOCK_TYPE_RECOMMEND_PRODUCT);

    @Resource
    private ShopCmsBlockDao shopCmsBlockDao;

    @Resource
    private ShopProductDao shopProductDao;

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
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getBlockType);
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getSort);
        queryWrapper.orderByDesc(ShopCmsBlockEntity::getBlockId);

        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopCmsBlockEntity> resultPage = shopCmsBlockDao.selectPage((Page<ShopCmsBlockEntity>) page, queryWrapper);
        List<ShopCmsBlockVO> blockList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopCmsBlockVO.class);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, blockList));
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
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getBlockType);
        queryWrapper.orderByAsc(ShopCmsBlockEntity::getSort);
        queryWrapper.orderByDesc(ShopCmsBlockEntity::getBlockId);
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
        blockEntity.setUpdateUserId(requestUser.getUserId());
        shopCmsBlockDao.updateById(blockEntity);
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

        ShopCmsBlockEntity updateEntity = new ShopCmsBlockEntity();
        updateEntity.setBlockId(disabledForm.getBlockId());
        updateEntity.setDisabledFlag(disabledForm.getDisabledFlag());
        updateEntity.setUpdateUserId(requestUser.getUserId());
        shopCmsBlockDao.updateById(updateEntity);
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
}
