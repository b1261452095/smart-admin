package net.lab1024.sa.admin.module.business.shop.category.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop category entity.
 */
@Data
@TableName("shop_category")
public class ShopCategoryEntity {

    @TableId(type = IdType.AUTO)
    private Long categoryId;

    private Long tenantId;

    private Long parentId;

    private String categoryName;

    private String categoryCode;

    private String categoryImage;

    private Integer sort;

    private Boolean disabledFlag;

    private Boolean deletedFlag;

    private String remark;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
