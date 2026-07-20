package net.lab1024.sa.admin.module.business.shop.inventory.domain.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop inventory record view object.
 */
@Data
public class ShopInventoryRecordVO {

    @Schema(description = "流水ID")
    private Long recordId;

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "商品ID")
    private Long productId;

    @Schema(description = "SKU ID")
    private Long skuId;

    @Schema(description = "商品名称")
    private String productName;

    @Schema(description = "SKU名称")
    private String skuName;

    @Schema(description = "SKU编码")
    private String skuCode;

    @Schema(description = "规格摘要")
    private String specSummary;

    @Schema(description = "操作类型")
    private Integer operationType;

    @Schema(description = "变动数量")
    private Integer changeQuantity;

    @Schema(description = "变动前可售库存")
    private Integer beforeAvailableStock;

    @Schema(description = "变动后可售库存")
    private Integer afterAvailableStock;

    @Schema(description = "变动前锁定库存")
    private Integer beforeLockedStock;

    @Schema(description = "变动后锁定库存")
    private Integer afterLockedStock;

    @Schema(description = "变动前已售库存")
    private Integer beforeSoldStock;

    @Schema(description = "变动后已售库存")
    private Integer afterSoldStock;

    @Schema(description = "备注")
    private String remark;

    @Schema(description = "创建人")
    private Long createUserId;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;
}
