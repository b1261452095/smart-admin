package net.lab1024.sa.admin.module.business.shop.customer.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * Shop customer login form.
 */
@Data
public class ShopCustomerLoginForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "账号：邮箱或手机号")
    @NotBlank(message = "账号不能为空")
    private String account;

    @Schema(description = "密码")
    @NotBlank(message = "密码不能为空")
    private String password;
}
