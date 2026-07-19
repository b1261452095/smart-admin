package net.lab1024.sa.admin.module.business.shop.category.domain.form;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import net.lab1024.sa.base.common.json.deserializer.FileKeyVoDeserializer;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;
import org.hibernate.validator.constraints.Length;

/**
 * Shop category add form.
 */
@Data
public class ShopCategoryAddForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "父级ID")
    private Long parentId;

    @Schema(description = "类目名称")
    @NotBlank(message = "类目名称不能为空")
    @Length(max = 100, message = "类目名称最多100字符")
    private String categoryName;

    @Schema(description = "类目编码")
    @Length(max = 100, message = "类目编码最多100字符")
    private String categoryCode;

    @Schema(description = "类目图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    @JsonDeserialize(using = FileKeyVoDeserializer.class)
    private String categoryImage;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;

    @Schema(description = "备注")
    @Length(max = 500, message = "备注最多500字符")
    private String remark;
}
