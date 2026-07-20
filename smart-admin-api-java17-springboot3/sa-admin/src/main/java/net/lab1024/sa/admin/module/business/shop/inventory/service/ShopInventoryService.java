package net.lab1024.sa.admin.module.business.shop.inventory.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.inventory.dao.ShopInventoryRecordDao;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.entity.ShopInventoryRecordEntity;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.form.ShopInventoryAdjustForm;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.form.ShopInventoryQueryForm;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.form.ShopInventoryRecordQueryForm;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.vo.ShopInventoryRecordVO;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.vo.ShopInventoryVO;
import net.lab1024.sa.admin.module.business.shop.product.dao.ShopProductDao;
import net.lab1024.sa.admin.module.business.shop.product.domain.entity.ShopProductEntity;
import net.lab1024.sa.admin.module.business.shop.sku.dao.ShopInventoryDao;
import net.lab1024.sa.admin.module.business.shop.sku.dao.ShopProductSkuDao;
import net.lab1024.sa.admin.module.business.shop.sku.domain.entity.ShopInventoryEntity;
import net.lab1024.sa.admin.module.business.shop.sku.domain.entity.ShopProductSkuEntity;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shop inventory service.
 */
@Service
public class ShopInventoryService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    private static final Integer OPERATION_TYPE_MANUAL_ADJUST = 1;

    @Resource
    private ShopInventoryDao shopInventoryDao;

    @Resource
    private ShopInventoryRecordDao shopInventoryRecordDao;

    @Resource
    private ShopProductDao shopProductDao;

    @Resource
    private ShopProductSkuDao shopProductSkuDao;

    /**
     * Query current SKU inventory page.
     */
    public ResponseDTO<PageResult<ShopInventoryVO>> queryPage(ShopInventoryQueryForm queryForm) {
        LambdaQueryWrapper<ShopInventoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopInventoryEntity::getTenantId, queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId());
        queryWrapper.eq(ShopInventoryEntity::getDeletedFlag, Boolean.FALSE);
        if (queryForm.getProductId() != null) {
            queryWrapper.eq(ShopInventoryEntity::getProductId, queryForm.getProductId());
        }
        if (Boolean.TRUE.equals(queryForm.getWarningFlag())) {
            queryWrapper.apply("available_stock <= warning_stock");
        }

        ResponseDTO<Boolean> searchResult = applyInventorySearch(queryWrapper, queryForm.getSearchWord());
        if (!searchResult.getOk()) {
            return ResponseDTO.ok(this.<ShopInventoryVO>emptyPage(queryForm));
        }

        queryWrapper.orderByDesc(ShopInventoryEntity::getUpdateTime);
        queryWrapper.orderByDesc(ShopInventoryEntity::getInventoryId);
        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopInventoryEntity> resultPage = shopInventoryDao.selectPage((Page<ShopInventoryEntity>) page, queryWrapper);
        List<ShopInventoryVO> inventoryList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopInventoryVO.class);
        fillInventoryInfo(inventoryList);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, inventoryList));
    }

    /**
     * Adjust available stock and write stock record.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> adjust(ShopInventoryAdjustForm adjustForm, RequestUser requestUser) {
        if (Objects.equals(adjustForm.getChangeQuantity(), 0)) {
            return ResponseDTO.userErrorParam("调整数量不能为0");
        }

        ShopInventoryEntity originInventory = getInventory(adjustForm.getSkuId());
        if (originInventory == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "库存不存在");
        }

        Integer beforeAvailableStock = originInventory.getAvailableStock() == null ? 0 : originInventory.getAvailableStock();
        Integer afterAvailableStock = beforeAvailableStock + adjustForm.getChangeQuantity();
        if (afterAvailableStock < 0) {
            return ResponseDTO.userErrorParam("可售库存不能小于0");
        }

        ShopInventoryEntity inventoryEntity = new ShopInventoryEntity();
        inventoryEntity.setInventoryId(originInventory.getInventoryId());
        inventoryEntity.setAvailableStock(afterAvailableStock);
        inventoryEntity.setWarningStock(adjustForm.getWarningStock() == null ? originInventory.getWarningStock() : adjustForm.getWarningStock());
        inventoryEntity.setUpdateUserId(requestUser.getUserId());
        shopInventoryDao.updateById(inventoryEntity);

        ShopInventoryRecordEntity recordEntity = new ShopInventoryRecordEntity();
        recordEntity.setTenantId(originInventory.getTenantId());
        recordEntity.setProductId(originInventory.getProductId());
        recordEntity.setSkuId(originInventory.getSkuId());
        recordEntity.setOperationType(OPERATION_TYPE_MANUAL_ADJUST);
        recordEntity.setChangeQuantity(adjustForm.getChangeQuantity());
        recordEntity.setBeforeAvailableStock(beforeAvailableStock);
        recordEntity.setAfterAvailableStock(afterAvailableStock);
        recordEntity.setBeforeLockedStock(originInventory.getLockedStock());
        recordEntity.setAfterLockedStock(originInventory.getLockedStock());
        recordEntity.setBeforeSoldStock(originInventory.getSoldStock());
        recordEntity.setAfterSoldStock(originInventory.getSoldStock());
        recordEntity.setRemark(adjustForm.getRemark());
        recordEntity.setCreateUserId(requestUser.getUserId());
        shopInventoryRecordDao.insert(recordEntity);
        return ResponseDTO.ok();
    }

    /**
     * Query inventory records.
     */
    public ResponseDTO<PageResult<ShopInventoryRecordVO>> queryRecordPage(ShopInventoryRecordQueryForm queryForm) {
        LambdaQueryWrapper<ShopInventoryRecordEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopInventoryRecordEntity::getTenantId, queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId());
        if (queryForm.getProductId() != null) {
            queryWrapper.eq(ShopInventoryRecordEntity::getProductId, queryForm.getProductId());
        }
        if (queryForm.getSkuId() != null) {
            queryWrapper.eq(ShopInventoryRecordEntity::getSkuId, queryForm.getSkuId());
        }
        if (queryForm.getOperationType() != null) {
            queryWrapper.eq(ShopInventoryRecordEntity::getOperationType, queryForm.getOperationType());
        }

        ResponseDTO<Boolean> searchResult = applyRecordSearch(queryWrapper, queryForm.getSearchWord());
        if (!searchResult.getOk()) {
            return ResponseDTO.ok(this.<ShopInventoryRecordVO>emptyPage(queryForm));
        }

        queryWrapper.orderByDesc(ShopInventoryRecordEntity::getRecordId);
        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopInventoryRecordEntity> resultPage = shopInventoryRecordDao.selectPage((Page<ShopInventoryRecordEntity>) page, queryWrapper);
        List<ShopInventoryRecordVO> recordList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopInventoryRecordVO.class);
        fillRecordInfo(recordList);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, recordList));
    }

    private ResponseDTO<Boolean> applyInventorySearch(LambdaQueryWrapper<ShopInventoryEntity> queryWrapper, String searchWord) {
        SearchIds searchIds = querySearchIds(searchWord);
        if (!searchIds.hasSearch) {
            return ResponseDTO.ok(Boolean.TRUE);
        }
        if (CollectionUtils.isEmpty(searchIds.productIdSet) && CollectionUtils.isEmpty(searchIds.skuIdSet)) {
            return ResponseDTO.userErrorParam("no data");
        }
        queryWrapper.and(wrapper -> {
            if (CollectionUtils.isNotEmpty(searchIds.productIdSet)) {
                wrapper.in(ShopInventoryEntity::getProductId, searchIds.productIdSet);
            }
            if (CollectionUtils.isNotEmpty(searchIds.productIdSet) && CollectionUtils.isNotEmpty(searchIds.skuIdSet)) {
                wrapper.or();
            }
            if (CollectionUtils.isNotEmpty(searchIds.skuIdSet)) {
                wrapper.in(ShopInventoryEntity::getSkuId, searchIds.skuIdSet);
            }
        });
        return ResponseDTO.ok(Boolean.TRUE);
    }

    private ResponseDTO<Boolean> applyRecordSearch(LambdaQueryWrapper<ShopInventoryRecordEntity> queryWrapper, String searchWord) {
        SearchIds searchIds = querySearchIds(searchWord);
        if (!searchIds.hasSearch) {
            return ResponseDTO.ok(Boolean.TRUE);
        }
        if (CollectionUtils.isEmpty(searchIds.productIdSet) && CollectionUtils.isEmpty(searchIds.skuIdSet)) {
            return ResponseDTO.userErrorParam("no data");
        }
        queryWrapper.and(wrapper -> {
            if (CollectionUtils.isNotEmpty(searchIds.productIdSet)) {
                wrapper.in(ShopInventoryRecordEntity::getProductId, searchIds.productIdSet);
            }
            if (CollectionUtils.isNotEmpty(searchIds.productIdSet) && CollectionUtils.isNotEmpty(searchIds.skuIdSet)) {
                wrapper.or();
            }
            if (CollectionUtils.isNotEmpty(searchIds.skuIdSet)) {
                wrapper.in(ShopInventoryRecordEntity::getSkuId, searchIds.skuIdSet);
            }
        });
        return ResponseDTO.ok(Boolean.TRUE);
    }

    private SearchIds querySearchIds(String searchWord) {
        SearchIds searchIds = new SearchIds();
        if (SmartStringUtil.isEmpty(searchWord)) {
            return searchIds;
        }
        searchIds.hasSearch = true;

        LambdaQueryWrapper<ShopProductEntity> productQuery = new LambdaQueryWrapper<>();
        productQuery.eq(ShopProductEntity::getDeletedFlag, Boolean.FALSE);
        productQuery.and(wrapper -> wrapper.like(ShopProductEntity::getProductName, searchWord)
                .or()
                .like(ShopProductEntity::getProductCode, searchWord));
        List<ShopProductEntity> productList = shopProductDao.selectList(productQuery);
        searchIds.productIdSet = productList.stream().map(ShopProductEntity::getProductId).collect(Collectors.toSet());

        LambdaQueryWrapper<ShopProductSkuEntity> skuQuery = new LambdaQueryWrapper<>();
        skuQuery.eq(ShopProductSkuEntity::getDeletedFlag, Boolean.FALSE);
        skuQuery.and(wrapper -> wrapper.like(ShopProductSkuEntity::getSkuName, searchWord)
                .or()
                .like(ShopProductSkuEntity::getSkuCode, searchWord)
                .or()
                .like(ShopProductSkuEntity::getSpecSummary, searchWord));
        List<ShopProductSkuEntity> skuList = shopProductSkuDao.selectList(skuQuery);
        searchIds.skuIdSet = skuList.stream().map(ShopProductSkuEntity::getSkuId).collect(Collectors.toSet());
        return searchIds;
    }

    private void fillInventoryInfo(List<ShopInventoryVO> inventoryList) {
        if (CollectionUtils.isEmpty(inventoryList)) {
            return;
        }
        Map<Long, ShopProductEntity> productMap = queryProductMap(inventoryList.stream().map(ShopInventoryVO::getProductId).collect(Collectors.toSet()));
        Map<Long, ShopProductSkuEntity> skuMap = querySkuMap(inventoryList.stream().map(ShopInventoryVO::getSkuId).collect(Collectors.toSet()));
        inventoryList.forEach(item -> {
            ShopProductEntity productEntity = productMap.get(item.getProductId());
            if (productEntity != null) {
                item.setProductName(productEntity.getProductName());
                item.setProductCode(productEntity.getProductCode());
            }
            ShopProductSkuEntity skuEntity = skuMap.get(item.getSkuId());
            if (skuEntity != null) {
                item.setSkuName(skuEntity.getSkuName());
                item.setSkuCode(skuEntity.getSkuCode());
                item.setSpecSummary(skuEntity.getSpecSummary());
                item.setCurrency(skuEntity.getCurrency());
                item.setSkuDisabledFlag(skuEntity.getDisabledFlag());
            }
            item.setWarningFlag(item.getWarningStock() != null && item.getAvailableStock() != null && item.getAvailableStock() <= item.getWarningStock());
        });
    }

    private void fillRecordInfo(List<ShopInventoryRecordVO> recordList) {
        if (CollectionUtils.isEmpty(recordList)) {
            return;
        }
        Map<Long, ShopProductEntity> productMap = queryProductMap(recordList.stream().map(ShopInventoryRecordVO::getProductId).collect(Collectors.toSet()));
        Map<Long, ShopProductSkuEntity> skuMap = querySkuMap(recordList.stream().map(ShopInventoryRecordVO::getSkuId).collect(Collectors.toSet()));
        recordList.forEach(item -> {
            ShopProductEntity productEntity = productMap.get(item.getProductId());
            if (productEntity != null) {
                item.setProductName(productEntity.getProductName());
            }
            ShopProductSkuEntity skuEntity = skuMap.get(item.getSkuId());
            if (skuEntity != null) {
                item.setSkuName(skuEntity.getSkuName());
                item.setSkuCode(skuEntity.getSkuCode());
                item.setSpecSummary(skuEntity.getSpecSummary());
            }
        });
    }

    private Map<Long, ShopProductEntity> queryProductMap(Set<Long> productIdSet) {
        if (CollectionUtils.isEmpty(productIdSet)) {
            return Collections.emptyMap();
        }
        List<ShopProductEntity> productList = shopProductDao.selectBatchIds(productIdSet);
        return productList.stream().collect(Collectors.toMap(ShopProductEntity::getProductId, item -> item));
    }

    private Map<Long, ShopProductSkuEntity> querySkuMap(Set<Long> skuIdSet) {
        if (CollectionUtils.isEmpty(skuIdSet)) {
            return Collections.emptyMap();
        }
        List<ShopProductSkuEntity> skuList = shopProductSkuDao.selectBatchIds(skuIdSet);
        return skuList.stream().collect(Collectors.toMap(ShopProductSkuEntity::getSkuId, item -> item));
    }

    private ShopInventoryEntity getInventory(Long skuId) {
        LambdaQueryWrapper<ShopInventoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopInventoryEntity::getSkuId, skuId);
        queryWrapper.eq(ShopInventoryEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopInventoryDao.selectOne(queryWrapper);
    }

    private <T> PageResult<T> emptyPage(ShopInventoryQueryForm queryForm) {
        PageResult<T> pageResult = new PageResult<>();
        pageResult.setPageNum(queryForm.getPageNum());
        pageResult.setPageSize(queryForm.getPageSize());
        pageResult.setPages(0L);
        pageResult.setTotal(0L);
        pageResult.setEmptyFlag(Boolean.TRUE);
        pageResult.setList(Collections.emptyList());
        return pageResult;
    }

    private <T> PageResult<T> emptyPage(ShopInventoryRecordQueryForm queryForm) {
        PageResult<T> pageResult = new PageResult<>();
        pageResult.setPageNum(queryForm.getPageNum());
        pageResult.setPageSize(queryForm.getPageSize());
        pageResult.setPages(0L);
        pageResult.setTotal(0L);
        pageResult.setEmptyFlag(Boolean.TRUE);
        pageResult.setList(Collections.emptyList());
        return pageResult;
    }

    private static class SearchIds {
        private boolean hasSearch;
        private Set<Long> productIdSet;
        private Set<Long> skuIdSet;
    }
}
