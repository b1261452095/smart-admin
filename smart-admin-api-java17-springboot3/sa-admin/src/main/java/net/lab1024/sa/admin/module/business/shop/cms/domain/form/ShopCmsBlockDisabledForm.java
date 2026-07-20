package net.lab1024.sa.admin.module.business.shop.cms.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * Shop CMS block disabled form.
 */
@Data
public class ShopCmsBlockDisabledForm {

    @Schema(description = "区块ID")
    @NotNull(message = "区块ID不能为空")
    private Long blockId;

    @Schema(description = "禁用状态")
    @NotNull(message = "禁用状态不能为空")
    private Boolean disabledFlag;
}
