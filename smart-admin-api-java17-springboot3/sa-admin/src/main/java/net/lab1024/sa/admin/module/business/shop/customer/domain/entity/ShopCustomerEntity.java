package net.lab1024.sa.admin.module.business.shop.customer.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop customer entity.
 */
@Data
@TableName("shop_customer")
public class ShopCustomerEntity {

    @TableId(type = IdType.AUTO)
    private Long customerId;

    private Long tenantId;

    private String customerNo;

    private String customerName;

    private String email;

    private String phone;

    private String loginPwd;

    private Integer registerSource;

    private Boolean disabledFlag;

    private Boolean deletedFlag;

    private LocalDateTime lastLoginTime;

    private String loginToken;

    private String remark;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
