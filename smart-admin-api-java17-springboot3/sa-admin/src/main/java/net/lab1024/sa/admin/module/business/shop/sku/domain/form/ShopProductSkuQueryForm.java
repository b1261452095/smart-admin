package net.lab1024.sa.admin.module.business.shop.sku.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * Shop SKU query form.
 */
@Data
public class ShopProductSkuQueryForm {

    @Schema(description = "商品ID")
    @NotNull(message = "商品ID不能为空")
    private Long productId;
}
