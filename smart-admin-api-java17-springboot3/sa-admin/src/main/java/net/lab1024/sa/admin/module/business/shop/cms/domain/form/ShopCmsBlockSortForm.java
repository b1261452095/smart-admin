package net.lab1024.sa.admin.module.business.shop.cms.domain.form;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

/**
 * CMS block sort form.
 */
@Data
public class ShopCmsBlockSortForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "按页面顺序排列的区块")
    @NotEmpty(message = "区块排序不能为空")
    @Size(max = 200, message = "单次最多排序200个区块")
    @Valid
    private List<SortItem> blockList;

    @Data
    public static class SortItem {

        @Schema(description = "区块ID")
        @NotNull(message = "区块ID不能为空")
        private Long blockId;

        @Schema(description = "排序")
        @NotNull(message = "排序不能为空")
        @Min(value = 0, message = "排序不能小于0")
        private Integer sort;

        @Schema(description = "数据版本")
        @NotNull(message = "数据版本不能为空")
        private Integer version;
    }
}
