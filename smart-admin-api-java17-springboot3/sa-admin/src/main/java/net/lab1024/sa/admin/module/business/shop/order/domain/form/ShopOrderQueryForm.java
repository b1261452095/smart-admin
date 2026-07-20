package net.lab1024.sa.admin.module.business.shop.order.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import net.lab1024.sa.base.common.domain.PageParam;

/**
 * Shop order query form.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ShopOrderQueryForm extends PageParam {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "订单号")
    private String orderNo;

    @Schema(description = "搜索关键字")
    private String searchWord;

    @Schema(description = "订单状态")
    private Integer orderStatus;

    @Schema(description = "支付状态")
    private Integer payStatus;

    @Schema(description = "发货状态")
    private Integer fulfillmentStatus;

    @Schema(description = "退款状态")
    private Integer refundStatus;
}
