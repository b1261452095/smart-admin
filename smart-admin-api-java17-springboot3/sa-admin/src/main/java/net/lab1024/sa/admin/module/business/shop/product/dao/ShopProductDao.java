package net.lab1024.sa.admin.module.business.shop.product.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.product.domain.entity.ShopProductEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop product dao.
 */
@Mapper
public interface ShopProductDao extends BaseMapper<ShopProductEntity> {
}
