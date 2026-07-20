package net.lab1024.sa.admin.module.business.shop.inventory.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.entity.ShopInventoryRecordEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop inventory record dao.
 */
@Mapper
public interface ShopInventoryRecordDao extends BaseMapper<ShopInventoryRecordEntity> {
}
