package net.lab1024.sa.admin.module.business.shop.order.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop order entity.
 */
@Data
@TableName("shop_order")
public class ShopOrderEntity {

    @TableId(type = IdType.AUTO)
    private Long orderId;

    private Long tenantId;

    private String orderNo;

    private Long customerId;

    private String customerName;

    private String customerEmail;

    private String customerPhone;

    private String countryCode;

    private String province;

    private String city;

    private String addressDetail;

    private String currency;

    private Long productAmountCent;

    private Long freightAmountCent;

    private Long discountAmountCent;

    private Long taxAmountCent;

    private Long payableAmountCent;

    private Long paidAmountCent;

    private Integer orderStatus;

    private Integer payStatus;

    private Integer fulfillmentStatus;

    private Integer refundStatus;

    private String cancelReason;

    private String buyerRemark;

    private String sellerRemark;

    private Boolean deletedFlag;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
