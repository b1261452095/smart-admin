package net.lab1024.sa.admin.module.business.shop.product.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.category.dao.ShopCategoryDao;
import net.lab1024.sa.admin.module.business.shop.category.domain.entity.ShopCategoryEntity;
import net.lab1024.sa.admin.module.business.shop.product.dao.ShopProductDao;
import net.lab1024.sa.admin.module.business.shop.product.domain.entity.ShopProductEntity;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductAddForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductQueryForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductShelvesForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductUpdateForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.vo.ShopProductVO;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import net.lab1024.sa.base.common.util.SmartPageUtil;
import net.lab1024.sa.base.common.util.SmartStringUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shop product service.
 */
@Service
public class ShopProductService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    @Resource
    private ShopProductDao shopProductDao;

    @Resource
    private ShopCategoryDao shopCategoryDao;

    /**
     * Query product page.
     */
    public ResponseDTO<PageResult<ShopProductVO>> queryPage(ShopProductQueryForm queryForm) {
        Long tenantId = queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId();
        LambdaQueryWrapper<ShopProductEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopProductEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopProductEntity::getDeletedFlag, Boolean.FALSE);
        if (queryForm.getCategoryId() != null) {
            queryWrapper.eq(ShopProductEntity::getCategoryId, queryForm.getCategoryId());
        }
        if (queryForm.getPublishStatus() != null) {
            queryWrapper.eq(ShopProductEntity::getPublishStatus, queryForm.getPublishStatus());
        }
        if (queryForm.getShelvesFlag() != null) {
            queryWrapper.eq(ShopProductEntity::getShelvesFlag, queryForm.getShelvesFlag());
        }
        if (SmartStringUtil.isNotEmpty(queryForm.getSearchWord())) {
            queryWrapper.and(wrapper -> wrapper.like(ShopProductEntity::getProductName, queryForm.getSearchWord())
                    .or()
                    .like(ShopProductEntity::getProductCode, queryForm.getSearchWord()));
        }
        queryWrapper.orderByAsc(ShopProductEntity::getSort);
        queryWrapper.orderByDesc(ShopProductEntity::getProductId);

        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopProductEntity> resultPage = shopProductDao.selectPage((Page<ShopProductEntity>) page, queryWrapper);
        List<ShopProductVO> productList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopProductVO.class);
        fillCategoryName(productList);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, productList));
    }

    /**
     * Get detail.
     */
    public ResponseDTO<ShopProductVO> get(Long productId) {
        ShopProductEntity productEntity = getValidProduct(productId);
        if (productEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }
        ShopProductVO productVO = SmartBeanUtil.copy(productEntity, ShopProductVO.class);
        fillCategoryName(List.of(productVO));
        return ResponseDTO.ok(productVO);
    }

    /**
     * Add product.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> add(ShopProductAddForm addForm, RequestUser requestUser) {
        Long tenantId = addForm.getTenantId() == null ? DEFAULT_TENANT_ID : addForm.getTenantId();
        ResponseDTO<String> checkResult = checkProduct(null, tenantId, addForm);
        if (!checkResult.getOk()) {
            return checkResult;
        }

        ShopProductEntity productEntity = SmartBeanUtil.copy(addForm, ShopProductEntity.class);
        normalize(productEntity);
        productEntity.setTenantId(tenantId);
        productEntity.setSort(productEntity.getSort() == null ? 0 : productEntity.getSort());
        productEntity.setDeletedFlag(Boolean.FALSE);
        productEntity.setCreateUserId(requestUser.getUserId());
        productEntity.setUpdateUserId(requestUser.getUserId());
        shopProductDao.insert(productEntity);
        return ResponseDTO.ok();
    }

    /**
     * Update product.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> update(ShopProductUpdateForm updateForm, RequestUser requestUser) {
        ShopProductEntity originEntity = getValidProduct(updateForm.getProductId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }
        ResponseDTO<String> checkResult = checkProduct(updateForm.getProductId(), originEntity.getTenantId(), updateForm);
        if (!checkResult.getOk()) {
            return checkResult;
        }

        ShopProductEntity productEntity = SmartBeanUtil.copy(updateForm, ShopProductEntity.class);
        normalize(productEntity);
        productEntity.setTenantId(originEntity.getTenantId());
        productEntity.setSort(productEntity.getSort() == null ? 0 : productEntity.getSort());
        productEntity.setUpdateUserId(requestUser.getUserId());
        shopProductDao.updateById(productEntity);
        return ResponseDTO.ok();
    }

    /**
     * Update shelves flag.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateShelves(ShopProductShelvesForm shelvesForm, RequestUser requestUser) {
        ShopProductEntity originEntity = getValidProduct(shelvesForm.getProductId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopProductEntity productEntity = new ShopProductEntity();
        productEntity.setProductId(shelvesForm.getProductId());
        productEntity.setShelvesFlag(shelvesForm.getShelvesFlag());
        productEntity.setUpdateUserId(requestUser.getUserId());
        shopProductDao.updateById(productEntity);
        return ResponseDTO.ok();
    }

    /**
     * Delete product.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> delete(Long productId, RequestUser requestUser) {
        ShopProductEntity originEntity = getValidProduct(productId);
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopProductEntity productEntity = new ShopProductEntity();
        productEntity.setProductId(productId);
        productEntity.setDeletedFlag(Boolean.TRUE);
        productEntity.setUpdateUserId(requestUser.getUserId());
        shopProductDao.updateById(productEntity);
        return ResponseDTO.ok();
    }

    private ResponseDTO<String> checkProduct(Long productId, Long tenantId, ShopProductAddForm productForm) {
        ShopCategoryEntity categoryEntity = shopCategoryDao.selectById(productForm.getCategoryId());
        if (categoryEntity == null || Boolean.TRUE.equals(categoryEntity.getDeletedFlag())) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "商品类目不存在");
        }
        if (Boolean.TRUE.equals(categoryEntity.getDisabledFlag())) {
            return ResponseDTO.userErrorParam("商品类目已禁用");
        }
        if (!Objects.equals(categoryEntity.getTenantId(), tenantId)) {
            return ResponseDTO.userErrorParam("商品类目租户不一致");
        }

        if (SmartStringUtil.isNotEmpty(productForm.getProductCode())) {
            LambdaQueryWrapper<ShopProductEntity> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.eq(ShopProductEntity::getTenantId, tenantId);
            queryWrapper.eq(ShopProductEntity::getProductCode, productForm.getProductCode());
            queryWrapper.eq(ShopProductEntity::getDeletedFlag, Boolean.FALSE);
            ShopProductEntity sameCodeProduct = shopProductDao.selectOne(queryWrapper);
            if (sameCodeProduct != null && !Objects.equals(productId, sameCodeProduct.getProductId())) {
                return ResponseDTO.userErrorParam("商品编码已存在");
            }
        }

        return ResponseDTO.ok();
    }

    private ShopProductEntity getValidProduct(Long productId) {
        if (productId == null) {
            return null;
        }
        LambdaQueryWrapper<ShopProductEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopProductEntity::getProductId, productId);
        queryWrapper.eq(ShopProductEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopProductDao.selectOne(queryWrapper);
    }

    private void normalize(ShopProductEntity productEntity) {
        if (SmartStringUtil.isEmpty(productEntity.getProductCode())) {
            productEntity.setProductCode(null);
        }
    }

    private void fillCategoryName(List<ShopProductVO> productList) {
        if (CollectionUtils.isEmpty(productList)) {
            return;
        }
        List<Long> categoryIdList = productList.stream()
                .map(ShopProductVO::getCategoryId)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(categoryIdList)) {
            return;
        }
        List<ShopCategoryEntity> categoryList = shopCategoryDao.selectBatchIds(categoryIdList);
        Map<Long, String> categoryNameMap = categoryList.stream().collect(Collectors.toMap(ShopCategoryEntity::getCategoryId, ShopCategoryEntity::getCategoryName));
        productList.forEach(product -> product.setCategoryName(categoryNameMap.get(product.getCategoryId())));
    }
}
