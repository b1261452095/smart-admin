package net.lab1024.sa.admin.module.business.shop.customer.domain.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

/**
 * Shop customer login result.
 */
@Data
public class ShopCustomerLoginVO {

    @Schema(description = "客户ID")
    private Long customerId;

    @Schema(description = "客户编号")
    private String customerNo;

    @Schema(description = "客户名称")
    private String customerName;

    @Schema(description = "邮箱")
    private String email;

    @Schema(description = "手机号")
    private String phone;

    @Schema(description = "登录token")
    private String token;
}
