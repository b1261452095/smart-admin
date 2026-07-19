package net.lab1024.sa.admin.module.business.shop.category.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

/**
 * Shop category tree query form.
 */
@Data
public class ShopCategoryTreeQueryForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "父级ID")
    private Long parentId;

    @Schema(description = "是否过滤禁用类目")
    private Boolean disabledFlag;
}
