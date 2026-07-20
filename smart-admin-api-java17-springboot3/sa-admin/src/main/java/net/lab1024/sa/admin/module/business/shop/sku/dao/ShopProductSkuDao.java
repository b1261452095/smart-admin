package net.lab1024.sa.admin.module.business.shop.sku.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.sku.domain.entity.ShopProductSkuEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop SKU dao.
 */
@Mapper
public interface ShopProductSkuDao extends BaseMapper<ShopProductSkuEntity> {
}
