package net.lab1024.sa.admin.module.business.shop.order.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.order.domain.entity.ShopOrderItemEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop order item dao.
 */
@Mapper
public interface ShopOrderItemDao extends BaseMapper<ShopOrderItemEntity> {
}
