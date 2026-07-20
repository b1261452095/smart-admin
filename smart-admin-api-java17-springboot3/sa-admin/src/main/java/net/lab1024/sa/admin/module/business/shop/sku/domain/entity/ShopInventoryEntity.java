package net.lab1024.sa.admin.module.business.shop.sku.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Current SKU inventory.
 */
@Data
@TableName("shop_inventory")
public class ShopInventoryEntity {

    @TableId(type = IdType.AUTO)
    private Long inventoryId;

    private Long tenantId;

    private Long productId;

    private Long skuId;

    private Integer availableStock;

    private Integer lockedStock;

    private Integer soldStock;

    private Integer warningStock;

    private Boolean deletedFlag;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
