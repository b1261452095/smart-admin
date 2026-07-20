package net.lab1024.sa.admin.module.business.shop.customer.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

/**
 * Shop customer remark form.
 */
@Data
public class ShopCustomerRemarkForm {

    @Schema(description = "客户ID")
    @NotNull(message = "客户ID不能为空")
    private Long customerId;

    @Schema(description = "备注")
    @Length(max = 500, message = "备注最多500字符")
    private String remark;
}
