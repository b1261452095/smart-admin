package net.lab1024.sa.admin.module.business.shop.order.domain.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Shop order view object.
 */
@Data
public class ShopOrderVO {

    @Schema(description = "订单ID")
    private Long orderId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "订单号")
    private String orderNo;

    @Schema(description = "客户ID")
    private Long customerId;

    @Schema(description = "客户名称")
    private String customerName;

    @Schema(description = "客户邮箱")
    private String customerEmail;

    @Schema(description = "客户手机")
    private String customerPhone;

    @Schema(description = "国家编码")
    private String countryCode;

    @Schema(description = "省/州")
    private String province;

    @Schema(description = "城市")
    private String city;

    @Schema(description = "详细地址")
    private String addressDetail;

    @Schema(description = "币种")
    private String currency;

    @Schema(description = "商品金额，单位分")
    private Long productAmountCent;

    @Schema(description = "运费，单位分")
    private Long freightAmountCent;

    @Schema(description = "优惠金额，单位分")
    private Long discountAmountCent;

    @Schema(description = "税费，单位分")
    private Long taxAmountCent;

    @Schema(description = "应付金额，单位分")
    private Long payableAmountCent;

    @Schema(description = "实付金额，单位分")
    private Long paidAmountCent;

    @Schema(description = "订单状态")
    private Integer orderStatus;

    @Schema(description = "支付状态")
    private Integer payStatus;

    @Schema(description = "发货状态")
    private Integer fulfillmentStatus;

    @Schema(description = "退款状态")
    private Integer refundStatus;

    @Schema(description = "取消原因")
    private String cancelReason;

    @Schema(description = "买家备注")
    private String buyerRemark;

    @Schema(description = "商家备注")
    private String sellerRemark;

    @Schema(description = "明细数量")
    private Integer itemCount;

    @Schema(description = "订单明细")
    private List<ShopOrderItemVO> itemList;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;
}
