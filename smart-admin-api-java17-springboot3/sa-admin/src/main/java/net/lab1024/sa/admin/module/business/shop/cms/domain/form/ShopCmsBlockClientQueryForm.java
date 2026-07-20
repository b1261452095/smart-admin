package net.lab1024.sa.admin.module.business.shop.cms.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

/**
 * Shop CMS block client query form.
 */
@Data
public class ShopCmsBlockClientQueryForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "区块类型：1首页Banner 2导航菜单 3推荐商品")
    private Integer blockType;
}
