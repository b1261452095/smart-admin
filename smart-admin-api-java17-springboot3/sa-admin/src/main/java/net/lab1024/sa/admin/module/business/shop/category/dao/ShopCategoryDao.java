package net.lab1024.sa.admin.module.business.shop.category.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import net.lab1024.sa.admin.module.business.shop.category.domain.entity.ShopCategoryEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * Shop category dao.
 */
@Mapper
public interface ShopCategoryDao extends BaseMapper<ShopCategoryEntity> {
}
