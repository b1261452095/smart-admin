package net.lab1024.sa.admin.module.business.shop.customer.domain.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop customer address view object.
 */
@Data
public class ShopCustomerAddressVO {

    @Schema(description = "地址ID")
    private Long addressId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "客户ID")
    private Long customerId;

    @Schema(description = "收货人")
    private String receiverName;

    @Schema(description = "收货电话")
    private String receiverPhone;

    @Schema(description = "国家/地区")
    private String countryCode;

    @Schema(description = "省")
    private String province;

    @Schema(description = "市")
    private String city;

    @Schema(description = "区")
    private String district;

    @Schema(description = "详细地址")
    private String addressDetail;

    @Schema(description = "邮编")
    private String postalCode;

    @Schema(description = "默认地址")
    private Boolean defaultFlag;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;
}
