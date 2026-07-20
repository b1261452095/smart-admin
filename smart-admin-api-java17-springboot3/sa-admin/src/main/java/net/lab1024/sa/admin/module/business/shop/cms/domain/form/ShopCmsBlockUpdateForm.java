package net.lab1024.sa.admin.module.business.shop.cms.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Shop CMS block update form.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ShopCmsBlockUpdateForm extends ShopCmsBlockAddForm {

    @Schema(description = "区块ID")
    @NotNull(message = "区块ID不能为空")
    private Long blockId;
}
