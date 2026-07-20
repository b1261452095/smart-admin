package net.lab1024.sa.admin.module.business.shop.inventory.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import net.lab1024.sa.base.common.domain.PageParam;

/**
 * Shop inventory query form.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ShopInventoryQueryForm extends PageParam {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "搜索关键字")
    private String searchWord;

    @Schema(description = "只看预警库存")
    private Boolean warningFlag;
}
