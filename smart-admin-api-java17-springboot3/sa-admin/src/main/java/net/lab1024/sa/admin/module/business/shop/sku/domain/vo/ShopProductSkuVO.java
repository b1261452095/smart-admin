package net.lab1024.sa.admin.module.business.shop.sku.domain.vo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;

import java.time.LocalDateTime;

/**
 * Shop SKU view object.
 */
@Data
public class ShopProductSkuVO {

    @Schema(description = "SKU ID")
    private Long skuId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "商品名称")
    private String productName;

    @Schema(description = "SKU名称")
    private String skuName;

    @Schema(description = "SKU编码")
    private String skuCode;

    @Schema(description = "规格JSON")
    private String specJson;

    @Schema(description = "规格摘要")
    private String specSummary;

    @Schema(description = "SKU图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    private String skuImage;

    @Schema(description = "售价，单位分")
    private Long salePriceCent;

    @Schema(description = "市场价，单位分")
    private Long marketPriceCent;

    @Schema(description = "成本价，单位分")
    private Long costPriceCent;

    @Schema(description = "币种")
    private String currency;

    @Schema(description = "可售库存")
    private Integer availableStock;

    @Schema(description = "锁定库存")
    private Integer lockedStock;

    @Schema(description = "已售库存")
    private Integer soldStock;

    @Schema(description = "预警库存")
    private Integer warningStock;

    @Schema(description = "禁用状态")
    private Boolean disabledFlag;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;
}
