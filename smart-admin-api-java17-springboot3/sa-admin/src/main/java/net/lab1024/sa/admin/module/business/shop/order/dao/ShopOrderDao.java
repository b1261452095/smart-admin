package net.lab1024.sa.admin.module.business.shop.order.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.order.domain.entity.ShopOrderEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop order dao.
 */
@Mapper
public interface ShopOrderDao extends BaseMapper<ShopOrderEntity> {
}
