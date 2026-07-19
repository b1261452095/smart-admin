package net.lab1024.sa.admin.module.business.shop.product.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import net.lab1024.sa.base.common.domain.PageParam;

/**
 * Shop product query form.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ShopProductQueryForm extends PageParam {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "类目ID")
    private Long categoryId;

    @Schema(description = "搜索关键字")
    private String searchWord;

    @Schema(description = "发布状态")
    private Integer publishStatus;

    @Schema(description = "上架状态")
    private Boolean shelvesFlag;
}
