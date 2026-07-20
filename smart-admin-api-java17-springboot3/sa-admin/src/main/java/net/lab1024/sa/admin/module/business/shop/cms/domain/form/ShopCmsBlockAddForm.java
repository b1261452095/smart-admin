package net.lab1024.sa.admin.module.business.shop.cms.domain.form;

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
 * Shop CMS block add form.
 */
@Data
public class ShopCmsBlockAddForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "区块类型：1首页Banner 2导航菜单 3推荐商品")
    @NotNull(message = "请选择区块类型")
    private Integer blockType;

    @Schema(description = "区块名称")
    @NotBlank(message = "区块名称不能为空")
    @Length(max = 100, message = "区块名称最多100字符")
    private String blockName;

    @Schema(description = "展示标题")
    @Length(max = 150, message = "展示标题最多150字符")
    private String blockTitle;

    @Schema(description = "展示副标题")
    @Length(max = 300, message = "展示副标题最多300字符")
    private String blockSubTitle;

    @Schema(description = "图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    @JsonDeserialize(using = FileKeyVoDeserializer.class)
    private String image;

    @Schema(description = "跳转链接")
    @Length(max = 500, message = "跳转链接最多500字符")
    private String linkUrl;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "扩展配置JSON")
    private String configJson;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;
}
