package net.lab1024.sa.admin.module.business.shop.category.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * Shop category update form.
 */
@Data
public class ShopCategoryUpdateForm extends ShopCategoryAddForm {

    @Schema(description = "类目ID")
    @NotNull(message = "类目ID不能为空")
    private Long categoryId;
}
