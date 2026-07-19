package net.lab1024.sa.admin.module.business.shop.category.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.category.dao.ShopCategoryDao;
import net.lab1024.sa.admin.module.business.shop.category.domain.entity.ShopCategoryEntity;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryAddForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryTreeQueryForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryUpdateDisabledForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryUpdateForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.vo.ShopCategoryVO;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shop category service.
 */
@Service
public class ShopCategoryService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    @Resource
    private ShopCategoryDao shopCategoryDao;

    /**
     * Query category tree.
     */
    public ResponseDTO<List<ShopCategoryVO>> queryTree(ShopCategoryTreeQueryForm queryForm) {
        Long tenantId = queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId();
        Long parentId = queryForm.getParentId() == null ? NumberUtils.LONG_ZERO : queryForm.getParentId();

        LambdaQueryWrapper<ShopCategoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCategoryEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCategoryEntity::getDeletedFlag, Boolean.FALSE);
        if (queryForm.getDisabledFlag() != null) {
            queryWrapper.eq(ShopCategoryEntity::getDisabledFlag, queryForm.getDisabledFlag());
        }
        queryWrapper.orderByAsc(ShopCategoryEntity::getSort);
        queryWrapper.orderByDesc(ShopCategoryEntity::getCategoryId);

        List<ShopCategoryEntity> categoryEntityList = shopCategoryDao.selectList(queryWrapper);
        List<ShopCategoryVO> categoryList = SmartBeanUtil.copyList(categoryEntityList, ShopCategoryVO.class);
        categoryList.forEach(item -> {
            item.setValue(item.getCategoryId());
            item.setLabel(item.getCategoryName());
        });

        Map<Long, List<ShopCategoryVO>> parentMap = categoryList.stream().collect(Collectors.groupingBy(ShopCategoryVO::getParentId));
        return ResponseDTO.ok(buildTree(parentMap, parentId));
    }

    /**
     * Query detail.
     */
    public ResponseDTO<ShopCategoryVO> get(Long categoryId) {
        ShopCategoryEntity categoryEntity = getValidCategory(categoryId);
        if (categoryEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }
        ShopCategoryVO categoryVO = SmartBeanUtil.copy(categoryEntity, ShopCategoryVO.class);
        categoryVO.setValue(categoryVO.getCategoryId());
        categoryVO.setLabel(categoryVO.getCategoryName());
        return ResponseDTO.ok(categoryVO);
    }

    /**
     * Add category.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> add(ShopCategoryAddForm addForm, RequestUser requestUser) {
        Long tenantId = addForm.getTenantId() == null ? DEFAULT_TENANT_ID : addForm.getTenantId();
        Long parentId = addForm.getParentId() == null ? NumberUtils.LONG_ZERO : addForm.getParentId();

        ResponseDTO<String> checkResult = checkCategory(null, tenantId, parentId, addForm.getCategoryName());
        if (!checkResult.getOk()) {
            return checkResult;
        }

        ShopCategoryEntity categoryEntity = SmartBeanUtil.copy(addForm, ShopCategoryEntity.class);
        categoryEntity.setTenantId(tenantId);
        categoryEntity.setParentId(parentId);
        categoryEntity.setSort(categoryEntity.getSort() == null ? 0 : categoryEntity.getSort());
        categoryEntity.setDisabledFlag(categoryEntity.getDisabledFlag() == null ? Boolean.FALSE : categoryEntity.getDisabledFlag());
        categoryEntity.setDeletedFlag(Boolean.FALSE);
        categoryEntity.setCreateUserId(requestUser.getUserId());
        categoryEntity.setUpdateUserId(requestUser.getUserId());
        shopCategoryDao.insert(categoryEntity);

        return ResponseDTO.ok();
    }

    /**
     * Update category.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> update(ShopCategoryUpdateForm updateForm, RequestUser requestUser) {
        ShopCategoryEntity originEntity = getValidCategory(updateForm.getCategoryId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        Long parentId = originEntity.getParentId() == null ? NumberUtils.LONG_ZERO : originEntity.getParentId();
        ResponseDTO<String> checkResult = checkCategory(updateForm.getCategoryId(), originEntity.getTenantId(), parentId, updateForm.getCategoryName());
        if (!checkResult.getOk()) {
            return checkResult;
        }

        ShopCategoryEntity categoryEntity = SmartBeanUtil.copy(updateForm, ShopCategoryEntity.class);
        categoryEntity.setTenantId(originEntity.getTenantId());
        categoryEntity.setParentId(originEntity.getParentId());
        categoryEntity.setSort(categoryEntity.getSort() == null ? 0 : categoryEntity.getSort());
        categoryEntity.setDisabledFlag(categoryEntity.getDisabledFlag() == null ? Boolean.FALSE : categoryEntity.getDisabledFlag());
        categoryEntity.setUpdateUserId(requestUser.getUserId());
        shopCategoryDao.updateById(categoryEntity);

        return ResponseDTO.ok();
    }

    /**
     * Update disabled flag.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateDisabled(ShopCategoryUpdateDisabledForm updateForm, RequestUser requestUser) {
        ShopCategoryEntity originEntity = getValidCategory(updateForm.getCategoryId());
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopCategoryEntity categoryEntity = new ShopCategoryEntity();
        categoryEntity.setCategoryId(updateForm.getCategoryId());
        categoryEntity.setDisabledFlag(updateForm.getDisabledFlag());
        categoryEntity.setUpdateUserId(requestUser.getUserId());
        shopCategoryDao.updateById(categoryEntity);

        return ResponseDTO.ok();
    }

    /**
     * Delete category. A category with child nodes cannot be deleted.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> delete(Long categoryId, RequestUser requestUser) {
        ShopCategoryEntity originEntity = getValidCategory(categoryId);
        if (originEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        LambdaQueryWrapper<ShopCategoryEntity> childQuery = new LambdaQueryWrapper<>();
        childQuery.eq(ShopCategoryEntity::getParentId, categoryId);
        childQuery.eq(ShopCategoryEntity::getDeletedFlag, Boolean.FALSE);
        List<ShopCategoryEntity> childList = shopCategoryDao.selectList(childQuery);
        if (CollectionUtils.isNotEmpty(childList)) {
            return ResponseDTO.userErrorParam("请先删除子级类目");
        }

        ShopCategoryEntity categoryEntity = new ShopCategoryEntity();
        categoryEntity.setCategoryId(categoryId);
        categoryEntity.setDeletedFlag(Boolean.TRUE);
        categoryEntity.setUpdateUserId(requestUser.getUserId());
        shopCategoryDao.updateById(categoryEntity);

        return ResponseDTO.ok();
    }

    private List<ShopCategoryVO> buildTree(Map<Long, List<ShopCategoryVO>> parentMap, Long parentId) {
        List<ShopCategoryVO> categoryList = parentMap.get(parentId);
        if (CollectionUtils.isEmpty(categoryList)) {
            return List.of();
        }
        categoryList.forEach(category -> category.setChildren(buildTree(parentMap, category.getCategoryId())));
        return categoryList;
    }

    private ResponseDTO<String> checkCategory(Long categoryId, Long tenantId, Long parentId, String categoryName) {
        if (!Objects.equals(parentId, NumberUtils.LONG_ZERO)) {
            ShopCategoryEntity parentEntity = getValidCategory(parentId);
            if (parentEntity == null) {
                return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "父级类目不存在");
            }
            if (!Objects.equals(parentEntity.getTenantId(), tenantId)) {
                return ResponseDTO.userErrorParam("父级类目租户不一致");
            }
        }

        LambdaQueryWrapper<ShopCategoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCategoryEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCategoryEntity::getParentId, parentId);
        queryWrapper.eq(ShopCategoryEntity::getCategoryName, categoryName);
        queryWrapper.eq(ShopCategoryEntity::getDeletedFlag, Boolean.FALSE);
        ShopCategoryEntity sameNameCategory = shopCategoryDao.selectOne(queryWrapper);
        if (sameNameCategory == null) {
            return ResponseDTO.ok();
        }
        if (categoryId != null && Objects.equals(categoryId, sameNameCategory.getCategoryId())) {
            return ResponseDTO.ok();
        }
        return ResponseDTO.userErrorParam("同级下已存在相同类目");
    }

    private ShopCategoryEntity getValidCategory(Long categoryId) {
        if (categoryId == null) {
            return null;
        }
        LambdaQueryWrapper<ShopCategoryEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCategoryEntity::getCategoryId, categoryId);
        queryWrapper.eq(ShopCategoryEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopCategoryDao.selectOne(queryWrapper);
    }
}
