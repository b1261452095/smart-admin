package net.lab1024.sa.admin.module.business.shop.setting.domain.vo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;

import java.time.LocalDateTime;

/**
 * Shop setting view object.
 */
@Data
public class ShopSettingVO {

    @Schema(description = "设置ID")
    private Long settingId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "店铺名称")
    private String storeName;

    @Schema(description = "店铺Logo")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String storeLogo;

    @Schema(description = "店铺域名")
    private String storeDomain;

    @Schema(description = "默认语言")
    private String defaultLanguage;

    @Schema(description = "默认币种")
    private String defaultCurrency;

    @Schema(description = "客服邮箱")
    private String supportEmail;

    @Schema(description = "是否启用税费")
    private Boolean taxEnabledFlag;

    @Schema(description = "是否启用结账")
    private Boolean checkoutEnabledFlag;

    @Schema(description = "是否维护中")
    private Boolean maintenanceEnabledFlag;

    @Schema(description = "SEO标题")
    private String seoTitle;

    @Schema(description = "SEO描述")
    private String seoDescription;

    @Schema(description = "备注")
    private String remark;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;
}
