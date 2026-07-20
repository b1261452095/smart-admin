package net.lab1024.sa.admin.module.business.shop.customer.domain.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Shop customer view object.
 */
@Data
public class ShopCustomerVO {

    @Schema(description = "客户ID")
    private Long customerId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "客户编号")
    private String customerNo;

    @Schema(description = "客户名称")
    private String customerName;

    @Schema(description = "邮箱")
    private String email;

    @Schema(description = "手机号")
    private String phone;

    @Schema(description = "注册来源")
    private Integer registerSource;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;

    @Schema(description = "最后登录时间")
    private LocalDateTime lastLoginTime;

    @Schema(description = "备注")
    private String remark;

    @Schema(description = "地址数量")
    private Integer addressCount;

    @Schema(description = "地址列表")
    private List<ShopCustomerAddressVO> addressList;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;
}
