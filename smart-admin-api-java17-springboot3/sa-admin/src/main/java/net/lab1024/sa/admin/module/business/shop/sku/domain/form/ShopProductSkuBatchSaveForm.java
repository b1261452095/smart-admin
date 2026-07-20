package net.lab1024.sa.admin.module.business.shop.sku.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;

/**
 * Batch save SKU list.
 */
@Data
public class ShopProductSkuBatchSaveForm {

    @Schema(description = "商品ID")
    @NotNull(message = "商品ID不能为空")
    private Long productId;

    @Schema(description = "SKU列表")
    @Valid
    @NotEmpty(message = "SKU列表不能为空")
    private List<ShopProductSkuSaveForm> skuList;
}
