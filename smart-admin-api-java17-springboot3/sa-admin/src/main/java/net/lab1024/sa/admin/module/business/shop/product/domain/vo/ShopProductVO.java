package net.lab1024.sa.admin.module.business.shop.product.domain.vo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;

import java.time.LocalDateTime;

/**
 * Shop product view object.
 */
@Data
public class ShopProductVO {

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "类目ID")
    private Long categoryId;

    @Schema(description = "类目名称")
    private String categoryName;

    @Schema(description = "商品名称")
    private String productName;

    @Schema(description = "商品编码")
    private String productCode;

    @Schema(description = "副标题")
    private String subTitle;

    @Schema(description = "主图")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String mainImage;

    @Schema(description = "详情图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String detailImages;

    @Schema(description = "售价，单位分")
    private Long salePriceCent;

    @Schema(description = "币种")
    private String currency;

    @Schema(description = "发布状态")
    private Integer publishStatus;

    @Schema(description = "上架状态")
    private Boolean shelvesFlag;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "SEO标题")
    private String seoTitle;

    @Schema(description = "SEO描述")
    private String seoDescription;

    @Schema(description = "商品详情")
    private String productDetail;

    @Schema(description = "备注")
    private String remark;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;
}
