package net.lab1024.sa.admin.module.business.shop.sku.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.product.dao.ShopProductDao;
import net.lab1024.sa.admin.module.business.shop.product.domain.entity.ShopProductEntity;
import net.lab1024.sa.admin.module.business.shop.sku.dao.ShopInventoryDao;
import net.lab1024.sa.admin.module.business.shop.sku.dao.ShopProductSkuDao;
import net.lab1024.sa.admin.module.business.shop.sku.domain.entity.ShopInventoryEntity;
import net.lab1024.sa.admin.module.business.shop.sku.domain.entity.ShopProductSkuEntity;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuBatchSaveForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuDisabledForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuSaveForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.vo.ShopProductSkuVO;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import net.lab1024.sa.base.common.util.SmartStringUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shop product SKU service.
 */
@Service
public class ShopProductSkuService {

    @Resource
    private ShopProductSkuDao shopProductSkuDao;

    @Resource
    private ShopInventoryDao shopInventoryDao;

    @Resource
    private ShopProductDao shopProductDao;

    /**
     * Query SKU list by product.
     */
    public ResponseDTO<List<ShopProductSkuVO>> queryList(Long productId) {
        ShopProductEntity productEntity = getValidProduct(productId);
        if (productEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        LambdaQueryWrapper<ShopProductSkuEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopProductSkuEntity::getProductId, productId);
        queryWrapper.eq(ShopProductSkuEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.orderByAsc(ShopProductSkuEntity::getSort);
        queryWrapper.orderByDesc(ShopProductSkuEntity::getSkuId);
        List<ShopProductSkuEntity> skuEntityList = shopProductSkuDao.selectList(queryWrapper);
        List<ShopProductSkuVO> skuList = SmartBeanUtil.copyList(skuEntityList, ShopProductSkuVO.class);
        fillInventory(productEntity, skuList);
        return ResponseDTO.ok(skuList);
    }

    /**
     * Save one SKU.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> save(ShopProductSkuSaveForm saveForm, RequestUser requestUser) {
        return saveOne(saveForm, requestUser);
    }

    /**
     * Batch save generated SKU list.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> saveList(ShopProductSkuBatchSaveForm saveForm, RequestUser requestUser) {
        Set<String> skuCodeSet = new HashSet<>();
        for (ShopProductSkuSaveForm skuForm : saveForm.getSkuList()) {
            skuForm.setProductId(saveForm.getProductId());
            if (SmartStringUtil.isNotEmpty(skuForm.getSkuCode()) && !skuCodeSet.add(skuForm.getSkuCode())) {
                return ResponseDTO.userErrorParam("SKU编码重复：" + skuForm.getSkuCode());
            }
        }

        for (ShopProductSkuSaveForm skuForm : saveForm.getSkuList()) {
            ResponseDTO<String> saveResult = saveOne(skuForm, requestUser);
            if (!saveResult.getOk()) {
                return saveResult;
            }
        }
        return ResponseDTO.ok();
    }

    /**
     * Update disabled flag.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateDisabled(ShopProductSkuDisabledForm disabledForm, RequestUser requestUser) {
        ShopProductSkuEntity originEntity = getValidSku(disabledForm.getSkuId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopProductSkuEntity skuEntity = new ShopProductSkuEntity();
        skuEntity.setSkuId(disabledForm.getSkuId());
        skuEntity.setDisabledFlag(disabledForm.getDisabledFlag());
        skuEntity.setUpdateUserId(requestUser.getUserId());
        shopProductSkuDao.updateById(skuEntity);
        return ResponseDTO.ok();
    }

    /**
     * Delete SKU.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> delete(Long skuId, RequestUser requestUser) {
        ShopProductSkuEntity originEntity = getValidSku(skuId);
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopProductSkuEntity skuEntity = new ShopProductSkuEntity();
        skuEntity.setSkuId(skuId);
        skuEntity.setDeletedFlag(Boolean.TRUE);
        skuEntity.setUpdateUserId(requestUser.getUserId());
        shopProductSkuDao.updateById(skuEntity);

        ShopInventoryEntity inventoryEntity = getInventory(skuId);
        if (inventoryEntity != null) {
            ShopInventoryEntity updateInventory = new ShopInventoryEntity();
            updateInventory.setInventoryId(inventoryEntity.getInventoryId());
            updateInventory.setDeletedFlag(Boolean.TRUE);
            updateInventory.setUpdateUserId(requestUser.getUserId());
            shopInventoryDao.updateById(updateInventory);
        }
        return ResponseDTO.ok();
    }

    private ResponseDTO<String> saveOne(ShopProductSkuSaveForm saveForm, RequestUser requestUser) {
        ShopProductEntity productEntity = getValidProduct(saveForm.getProductId());
        if (productEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "商品不存在");
        }

        ShopProductSkuEntity originEntity = null;
        if (saveForm.getSkuId() != null) {
            originEntity = getValidSku(saveForm.getSkuId());
            if (originEntity == null) {
                return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "SKU不存在");
            }
            if (!Objects.equals(originEntity.getProductId(), saveForm.getProductId())) {
                return ResponseDTO.userErrorParam("SKU不属于当前商品");
            }
        }

        ResponseDTO<String> checkResult = checkSkuCode(saveForm, productEntity.getTenantId());
        if (!checkResult.getOk()) {
            return checkResult;
        }

        ShopProductSkuEntity skuEntity = SmartBeanUtil.copy(saveForm, ShopProductSkuEntity.class);
        normalize(skuEntity);
        skuEntity.setTenantId(productEntity.getTenantId());
        skuEntity.setProductId(productEntity.getProductId());
        skuEntity.setSort(skuEntity.getSort() == null ? 0 : skuEntity.getSort());
        skuEntity.setDisabledFlag(skuEntity.getDisabledFlag() == null ? Boolean.FALSE : skuEntity.getDisabledFlag());
        skuEntity.setDeletedFlag(Boolean.FALSE);
        skuEntity.setUpdateUserId(requestUser.getUserId());
        if (originEntity == null) {
            skuEntity.setCreateUserId(requestUser.getUserId());
            shopProductSkuDao.insert(skuEntity);
        } else {
            shopProductSkuDao.updateById(skuEntity);
        }

        saveInventory(productEntity, skuEntity.getSkuId(), saveForm, requestUser);
        return ResponseDTO.ok();
    }

    private ResponseDTO<String> checkSkuCode(ShopProductSkuSaveForm saveForm, Long tenantId) {
        if (SmartStringUtil.isEmpty(saveForm.getSkuCode())) {
            return ResponseDTO.ok();
        }

        LambdaQueryWrapper<ShopProductSkuEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopProductSkuEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopProductSkuEntity::getProductId, saveForm.getProductId());
        queryWrapper.eq(ShopProductSkuEntity::getSkuCode, saveForm.getSkuCode());
        queryWrapper.eq(ShopProductSkuEntity::getDeletedFlag, Boolean.FALSE);
        ShopProductSkuEntity sameCodeSku = shopProductSkuDao.selectOne(queryWrapper);
        if (sameCodeSku != null && !Objects.equals(saveForm.getSkuId(), sameCodeSku.getSkuId())) {
            return ResponseDTO.userErrorParam("SKU编码已存在：" + saveForm.getSkuCode());
        }
        return ResponseDTO.ok();
    }

    private void saveInventory(ShopProductEntity productEntity, Long skuId, ShopProductSkuSaveForm saveForm, RequestUser requestUser) {
        ShopInventoryEntity originInventory = getInventory(skuId);
        ShopInventoryEntity inventoryEntity = new ShopInventoryEntity();
        if (originInventory != null) {
            inventoryEntity.setInventoryId(originInventory.getInventoryId());
            inventoryEntity.setLockedStock(originInventory.getLockedStock());
            inventoryEntity.setSoldStock(originInventory.getSoldStock());
        } else {
            inventoryEntity.setLockedStock(0);
            inventoryEntity.setSoldStock(0);
            inventoryEntity.setCreateUserId(requestUser.getUserId());
        }
        inventoryEntity.setTenantId(productEntity.getTenantId());
        inventoryEntity.setProductId(productEntity.getProductId());
        inventoryEntity.setSkuId(skuId);
        inventoryEntity.setAvailableStock(saveForm.getAvailableStock());
        inventoryEntity.setWarningStock(saveForm.getWarningStock());
        inventoryEntity.setDeletedFlag(Boolean.FALSE);
        inventoryEntity.setUpdateUserId(requestUser.getUserId());
        if (originInventory == null) {
            shopInventoryDao.insert(inventoryEntity);
        } else {
            shopInventoryDao.updateById(inventoryEntity);
        }
    }

    private void fillInventory(ShopProductEntity productEntity, List<ShopProductSkuVO> skuList) {
        if (CollectionUtils.isEmpty(skuList)) {
            return;
        }
        skuList.forEach(sku -> sku.setProductName(productEntity.getProductName()));

        List<Long> skuIdList = skuList.stream().map(ShopProductSkuVO::getSkuId).collect(Collectors.toList());
        LambdaQueryWrapper<ShopInventoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(ShopInventoryEntity::getSkuId, skuIdList);
        queryWrapper.eq(ShopInventoryEntity::getDeletedFlag, Boolean.FALSE);
        List<ShopInventoryEntity> inventoryList = shopInventoryDao.selectList(queryWrapper);
        Map<Long, ShopInventoryEntity> inventoryMap = inventoryList.stream().collect(Collectors.toMap(ShopInventoryEntity::getSkuId, item -> item));
        skuList.forEach(sku -> {
            ShopInventoryEntity inventoryEntity = inventoryMap.get(sku.getSkuId());
            if (inventoryEntity == null) {
                sku.setAvailableStock(0);
                sku.setLockedStock(0);
                sku.setSoldStock(0);
                sku.setWarningStock(0);
                return;
            }
            sku.setAvailableStock(inventoryEntity.getAvailableStock());
            sku.setLockedStock(inventoryEntity.getLockedStock());
            sku.setSoldStock(inventoryEntity.getSoldStock());
            sku.setWarningStock(inventoryEntity.getWarningStock());
        });
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

    private ShopProductSkuEntity getValidSku(Long skuId) {
        if (skuId == null) {
            return null;
        }
        LambdaQueryWrapper<ShopProductSkuEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopProductSkuEntity::getSkuId, skuId);
        queryWrapper.eq(ShopProductSkuEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopProductSkuDao.selectOne(queryWrapper);
    }

    private ShopInventoryEntity getInventory(Long skuId) {
        LambdaQueryWrapper<ShopInventoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopInventoryEntity::getSkuId, skuId);
        queryWrapper.eq(ShopInventoryEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopInventoryDao.selectOne(queryWrapper);
    }

    private void normalize(ShopProductSkuEntity skuEntity) {
        if (SmartStringUtil.isEmpty(skuEntity.getSkuCode())) {
            skuEntity.setSkuCode(null);
        }
    }
}
