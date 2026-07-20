package net.lab1024.sa.admin.module.business.shop.inventory.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop inventory change record.
 */
@Data
@TableName("shop_inventory_record")
public class ShopInventoryRecordEntity {

    @TableId(type = IdType.AUTO)
    private Long recordId;

    private Long tenantId;

    private Long productId;

    private Long skuId;

    private Integer operationType;

    private Integer changeQuantity;

    private Integer beforeAvailableStock;

    private Integer afterAvailableStock;

    private Integer beforeLockedStock;

    private Integer afterLockedStock;

    private Integer beforeSoldStock;

    private Integer afterSoldStock;

    private String remark;

    private Long createUserId;

    private LocalDateTime createTime;
}
