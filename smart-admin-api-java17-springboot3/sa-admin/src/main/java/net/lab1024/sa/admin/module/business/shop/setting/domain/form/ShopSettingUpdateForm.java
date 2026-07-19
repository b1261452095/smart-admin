package net.lab1024.sa.admin.module.business.shop.setting.domain.form;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import net.lab1024.sa.base.common.json.deserializer.FileKeyVoDeserializer;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;
import org.hibernate.validator.constraints.Length;

/**
 * Shop setting update form.
 */
@Data
public class ShopSettingUpdateForm {

    @Schema(description = "设置ID")
    private Long settingId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "店铺名称")
    @NotBlank(message = "店铺名称不能为空")
    @Length(max = 100, message = "店铺名称最多100字符")
    private String storeName;

    @Schema(description = "店铺Logo")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    @JsonDeserialize(using = FileKeyVoDeserializer.class)
    private String storeLogo;

    @Schema(description = "店铺域名")
    @Length(max = 200, message = "店铺域名最多200字符")
    private String storeDomain;

    @Schema(description = "默认语言")
    @NotBlank(message = "默认语言不能为空")
    @Length(max = 20, message = "默认语言最多20字符")
    private String defaultLanguage;

    @Schema(description = "默认币种")
    @NotBlank(message = "默认币种不能为空")
    @Length(max = 20, message = "默认币种最多20字符")
    private String defaultCurrency;

    @Schema(description = "客服邮箱")
    @Length(max = 100, message = "客服邮箱最多100字符")
    private String supportEmail;

    @Schema(description = "是否启用税费")
    @NotNull(message = "是否启用税费不能为空")
    private Boolean taxEnabledFlag;

    @Schema(description = "是否启用结账")
    @NotNull(message = "是否启用结账不能为空")
    private Boolean checkoutEnabledFlag;

    @Schema(description = "是否维护中")
    @NotNull(message = "是否维护中不能为空")
    private Boolean maintenanceEnabledFlag;

    @Schema(description = "SEO标题")
    @Length(max = 200, message = "SEO标题最多200字符")
    private String seoTitle;

    @Schema(description = "SEO描述")
    @Length(max = 500, message = "SEO描述最多500字符")
    private String seoDescription;

    @Schema(description = "备注")
    @Length(max = 500, message = "备注最多500字符")
    private String remark;
}
