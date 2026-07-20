package net.lab1024.sa.admin.module.business.shop.order.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop order item entity.
 */
@Data
@TableName("shop_order_item")
public class ShopOrderItemEntity {

    @TableId(type = IdType.AUTO)
    private Long orderItemId;

    private Long tenantId;

    private Long orderId;

    private Long productId;

    private Long skuId;

    private String productName;

    private String skuName;

    private String skuCode;

    private String specSummary;

    private String productImage;

    private Long salePriceCent;

    private Integer quantity;

    private Long totalAmountCent;

    private String currency;

    private Boolean deletedFlag;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
