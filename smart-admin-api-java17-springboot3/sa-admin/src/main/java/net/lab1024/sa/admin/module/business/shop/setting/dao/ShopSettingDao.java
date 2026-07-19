package net.lab1024.sa.admin.module.business.shop.setting.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.setting.domain.entity.ShopSettingEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop setting dao.
 */
@Mapper
public interface ShopSettingDao extends BaseMapper<ShopSettingEntity> {
}
