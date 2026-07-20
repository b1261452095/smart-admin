package net.lab1024.sa.admin.module.business.shop.customer.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop customer address entity.
 */
@Data
@TableName("shop_customer_address")
public class ShopCustomerAddressEntity {

    @TableId(type = IdType.AUTO)
    private Long addressId;

    private Long tenantId;

    private Long customerId;

    private String receiverName;

    private String receiverPhone;

    private String countryCode;

    private String province;

    private String city;

    private String district;

    private String addressDetail;

    private String postalCode;

    private Boolean defaultFlag;

    private Boolean deletedFlag;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
