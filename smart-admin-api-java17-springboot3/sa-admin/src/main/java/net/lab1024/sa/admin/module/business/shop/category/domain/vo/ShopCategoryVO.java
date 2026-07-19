package net.lab1024.sa.admin.module.business.shop.category.domain.vo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Shop category view object.
 */
@Data
public class ShopCategoryVO {

    @Schema(description = "类目ID")
    private Long categoryId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "父级ID")
    private Long parentId;

    @Schema(description = "类目名称")
    private String categoryName;

    @Schema(description = "类目编码")
    private String categoryCode;

    @Schema(description = "类目图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String categoryImage;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;

    @Schema(description = "备注")
    private String remark;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;

    @Schema(description = "树选择值")
    private Long value;

    @Schema(description = "树选择标签")
    private String label;

    @Schema(description = "子类目")
    private List<ShopCategoryVO> children;
}
