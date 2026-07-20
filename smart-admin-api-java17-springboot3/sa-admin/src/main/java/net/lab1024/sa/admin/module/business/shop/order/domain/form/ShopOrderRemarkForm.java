package net.lab1024.sa.admin.module.business.shop.order.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

/**
 * Update shop order seller remark.
 */
@Data
public class ShopOrderRemarkForm {

    @Schema(description = "订单ID")
    @NotNull(message = "订单ID不能为空")
    private Long orderId;

    @Schema(description = "商家备注")
    @Length(max = 500, message = "商家备注最多500字符")
    private String sellerRemark;
}
