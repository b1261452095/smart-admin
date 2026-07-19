package net.lab1024.sa.admin.module.business.shop.setting.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.setting.dao.ShopSettingDao;
import net.lab1024.sa.admin.module.business.shop.setting.domain.entity.ShopSettingEntity;
import net.lab1024.sa.admin.module.business.shop.setting.domain.form.ShopSettingUpdateForm;
import net.lab1024.sa.admin.module.business.shop.setting.domain.vo.ShopSettingVO;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Shop setting service.
 */
@Service
public class ShopSettingService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    @Resource
    private ShopSettingDao shopSettingDao;

    /**
     * Get current shop setting.
     */
    public ResponseDTO<ShopSettingVO> get() {
        ShopSettingEntity settingEntity = getSettingEntity(DEFAULT_TENANT_ID);
        if (settingEntity == null) {
            settingEntity = buildDefaultSetting();
        }
        return ResponseDTO.ok(SmartBeanUtil.copy(settingEntity, ShopSettingVO.class));
    }

    /**
     * Update current shop setting. Insert default tenant setting when it does not exist.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> update(ShopSettingUpdateForm updateForm, RequestUser requestUser) {
        Long tenantId = updateForm.getTenantId() == null ? DEFAULT_TENANT_ID : updateForm.getTenantId();
        ShopSettingEntity originEntity = getSettingEntity(tenantId);
        ShopSettingEntity settingEntity = SmartBeanUtil.copy(updateForm, ShopSettingEntity.class);
        settingEntity.setTenantId(tenantId);
        settingEntity.setDeletedFlag(Boolean.FALSE);
        settingEntity.setUpdateUserId(requestUser.getUserId());

        if (originEntity == null) {
            settingEntity.setCreateUserId(requestUser.getUserId());
            shopSettingDao.insert(settingEntity);
        } else {
            settingEntity.setSettingId(originEntity.getSettingId());
            shopSettingDao.updateById(settingEntity);
        }

        return ResponseDTO.ok();
    }

    private ShopSettingEntity getSettingEntity(Long tenantId) {
        LambdaQueryWrapper<ShopSettingEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopSettingEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopSettingEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopSettingDao.selectOne(queryWrapper);
    }

    private ShopSettingEntity buildDefaultSetting() {
        ShopSettingEntity settingEntity = new ShopSettingEntity();
        settingEntity.setTenantId(DEFAULT_TENANT_ID);
        settingEntity.setStoreName("Smart Shop");
        settingEntity.setDefaultLanguage("zh-CN");
        settingEntity.setDefaultCurrency("USD");
        settingEntity.setTaxEnabledFlag(Boolean.FALSE);
        settingEntity.setCheckoutEnabledFlag(Boolean.TRUE);
        settingEntity.setMaintenanceEnabledFlag(Boolean.FALSE);
        settingEntity.setDeletedFlag(Boolean.FALSE);
        return settingEntity;
    }
}
