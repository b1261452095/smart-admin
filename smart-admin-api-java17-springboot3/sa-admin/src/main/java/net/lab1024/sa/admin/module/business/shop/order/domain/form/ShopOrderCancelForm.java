package net.lab1024.sa.admin.module.business.shop.order.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

/**
 * Cancel shop order.
 */
@Data
public class ShopOrderCancelForm {

    @Schema(description = "订单ID")
    @NotNull(message = "订单ID不能为空")
    private Long orderId;

    @Schema(description = "取消原因")
    @NotBlank(message = "取消原因不能为空")
    @Length(max = 500, message = "取消原因最多500字符")
    private String cancelReason;
}
