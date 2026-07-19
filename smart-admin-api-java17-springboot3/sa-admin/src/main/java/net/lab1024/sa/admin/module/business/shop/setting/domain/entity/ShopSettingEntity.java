package net.lab1024.sa.admin.module.business.shop.setting.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop setting entity.
 */
@Data
@TableName("shop_setting")
public class ShopSettingEntity {

    @TableId(type = IdType.AUTO)
    private Long settingId;

    private Long tenantId;

    private String storeName;

    private String storeLogo;

    private String storeDomain;

    private String defaultLanguage;

    private String defaultCurrency;

    private String supportEmail;

    private Boolean taxEnabledFlag;

    private Boolean checkoutEnabledFlag;

    private Boolean maintenanceEnabledFlag;

    private String seoTitle;

    private String seoDescription;

    private String remark;

    private Boolean deletedFlag;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
