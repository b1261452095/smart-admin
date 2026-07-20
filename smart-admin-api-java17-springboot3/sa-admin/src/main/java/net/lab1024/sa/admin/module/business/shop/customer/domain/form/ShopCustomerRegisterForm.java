package net.lab1024.sa.admin.module.business.shop.customer.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

/**
 * Shop customer register form.
 */
@Data
public class ShopCustomerRegisterForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "客户名称")
    @Length(max = 100, message = "客户名称最多100字符")
    private String customerName;

    @Schema(description = "邮箱")
    @Length(max = 150, message = "邮箱最多150字符")
    private String email;

    @Schema(description = "手机号")
    @Length(max = 50, message = "手机号最多50字符")
    private String phone;

    @Schema(description = "密码")
    @NotBlank(message = "密码不能为空")
    @Length(min = 6, max = 50, message = "密码长度为6-50字符")
    private String password;
}
