package net.lab1024.sa.admin.module.business.shop.inventory.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

/**
 * Shop inventory adjust form.
 */
@Data
public class ShopInventoryAdjustForm {

    @Schema(description = "SKU ID")
    @NotNull(message = "SKU ID不能为空")
    private Long skuId;

    @Schema(description = "调整数量，正数入库，负数出库")
    @NotNull(message = "调整数量不能为空")
    private Integer changeQuantity;

    @Schema(description = "预警库存")
    @Min(value = 0, message = "预警库存不能小于0")
    private Integer warningStock;

    @Schema(description = "备注")
    @Length(max = 500, message = "备注最多500字符")
    private String remark;
}
