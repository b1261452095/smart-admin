package net.lab1024.sa.admin.module.business.shop.customer.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.customer.domain.entity.ShopCustomerAddressEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop customer address dao.
 */
@Mapper
public interface ShopCustomerAddressDao extends BaseMapper<ShopCustomerAddressEntity> {
}
