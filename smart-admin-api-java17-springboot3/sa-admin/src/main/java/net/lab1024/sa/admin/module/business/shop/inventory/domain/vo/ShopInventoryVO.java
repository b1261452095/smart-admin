package net.lab1024.sa.admin.module.business.shop.inventory.domain.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop inventory view object.
 */
@Data
public class ShopInventoryVO {

    @Schema(description = "库存ID")
    private Long inventoryId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "SKU ID")
    private Long skuId;

    @Schema(description = "商品名称")
    private String productName;

    @Schema(description = "商品编码")
    private String productCode;

    @Schema(description = "SKU名称")
    private String skuName;

    @Schema(description = "SKU编码")
    private String skuCode;

    @Schema(description = "规格摘要")
    private String specSummary;

    @Schema(description = "币种")
    private String currency;

    @Schema(description = "SKU禁用状态")
    private Boolean skuDisabledFlag;

    @Schema(description = "可售库存")
    private Integer availableStock;

    @Schema(description = "锁定库存")
    private Integer lockedStock;

    @Schema(description = "已售库存")
    private Integer soldStock;

    @Schema(description = "预警库存")
    private Integer warningStock;

    @Schema(description = "是否预警")
    private Boolean warningFlag;

    @Schema(description = "更新时间")
    private LocalDateTime updateTime;
}
