package net.lab1024.sa.admin.module.business.shop.cms.domain.vo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;

import java.time.LocalDateTime;

/**
 * Shop CMS block view object.
 */
@Data
public class ShopCmsBlockVO {

    @Schema(description = "区块ID")
    private Long blockId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "区块类型")
    private Integer blockType;

    @Schema(description = "区块名称")
    private String blockName;

    @Schema(description = "展示标题")
    private String blockTitle;

    @Schema(description = "展示副标题")
    private String blockSubTitle;

    @Schema(description = "图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String image;

    @Schema(description = "跳转链接")
    private String linkUrl;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "商品名称")
    private String productName;

    @Schema(description = "扩展配置JSON")
    private String configJson;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;

    @Schema(description = "数据版本")
    private Integer version;
}
