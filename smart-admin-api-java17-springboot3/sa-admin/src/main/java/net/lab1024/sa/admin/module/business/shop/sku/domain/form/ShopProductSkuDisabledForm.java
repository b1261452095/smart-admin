package net.lab1024.sa.admin.module.business.shop.sku.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * Update SKU disabled flag.
 */
@Data
public class ShopProductSkuDisabledForm {

    @Schema(description = "SKU ID")
    @NotNull(message = "SKU ID不能为空")
    private Long skuId;

    @Schema(description = "禁用状态")
    @NotNull(message = "禁用状态不能为空")
    private Boolean disabledFlag;
}
