package net.lab1024.sa.admin.module.business.shop.cms.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import net.lab1024.sa.base.common.domain.PageParam;

/**
 * Shop CMS block query form.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ShopCmsBlockQueryForm extends PageParam {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "区块类型：1首页Banner 2导航菜单 3推荐商品")
    private Integer blockType;

    @Schema(description = "搜索关键字")
    private String searchWord;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;
}
