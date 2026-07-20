package net.lab1024.sa.admin.module.business.shop.sku.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.sku.domain.entity.ShopInventoryEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop inventory dao.
 */
@Mapper
public interface ShopInventoryDao extends BaseMapper<ShopInventoryEntity> {
}
