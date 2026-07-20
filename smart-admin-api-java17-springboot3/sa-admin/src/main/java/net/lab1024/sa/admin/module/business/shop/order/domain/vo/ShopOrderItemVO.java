package net.lab1024.sa.admin.module.business.shop.order.domain.vo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;

/**
 * Shop order item view object.
 */
@Data
public class ShopOrderItemVO {

    @Schema(description = "订单明细ID")
    private Long orderItemId;

    @Schema(description = "订单ID")
    private Long orderId;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "SKU ID")
    private Long skuId;

    @Schema(description = "商品名称")
    private String productName;

    @Schema(description = "SKU名称")
    private String skuName;

    @Schema(description = "SKU编码")
    private String skuCode;

    @Schema(description = "规格摘要")
    private String specSummary;

    @Schema(description = "商品图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String productImage;

    @Schema(description = "单价，单位分")
    private Long salePriceCent;

    @Schema(description = "数量")
    private Integer quantity;

    @Schema(description = "小计，单位分")
    private Long totalAmountCent;

    @Schema(description = "币种")
    private String currency;
}
