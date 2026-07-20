package net.lab1024.sa.admin.module.business.shop.sku.domain.form;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import net.lab1024.sa.base.common.json.deserializer.FileKeyVoDeserializer;
import net.lab1024.sa.base.common.json.serializer.FileKeyVoSerializer;
import org.hibernate.validator.constraints.Length;

/**
 * Shop SKU save form.
 */
@Data
public class ShopProductSkuSaveForm {

    @Schema(description = "SKU ID")
    private Long skuId;

    @Schema(description = "商品ID")
    @NotNull(message = "商品ID不能为空")
    private Long productId;

    @Schema(description = "SKU名称")
    @NotBlank(message = "SKU名称不能为空")
    @Length(max = 200, message = "SKU名称最多200字符")
    private String skuName;

    @Schema(description = "SKU编码")
    @Length(max = 100, message = "SKU编码最多100字符")
    private String skuCode;

    @Schema(description = "规格JSON")
    private String specJson;

    @Schema(description = "规格摘要")
    @Length(max = 500, message = "规格摘要最多500字符")
    private String specSummary;

    @Schema(description = "SKU图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    @JsonDeserialize(using = FileKeyVoDeserializer.class)
    private String skuImage;

    @Schema(description = "售价，单位分")
    @NotNull(message = "售价不能为空")
    @Min(value = 0, message = "售价不能小于0")
    private Long salePriceCent;

    @Schema(description = "市场价，单位分")
    @Min(value = 0, message = "市场价不能小于0")
    private Long marketPriceCent;

    @Schema(description = "成本价，单位分")
    @Min(value = 0, message = "成本价不能小于0")
    private Long costPriceCent;

    @Schema(description = "币种")
    @NotBlank(message = "币种不能为空")
    @Length(max = 20, message = "币种最多20字符")
    private String currency;

    @Schema(description = "可售库存")
    @NotNull(message = "可售库存不能为空")
    @Min(value = 0, message = "可售库存不能小于0")
    private Integer availableStock;

    @Schema(description = "预警库存")
    @NotNull(message = "预警库存不能为空")
    @Min(value = 0, message = "预警库存不能小于0")
    private Integer warningStock;

    @Schema(description = "禁用状态")
    @NotNull(message = "禁用状态不能为空")
    private Boolean disabledFlag;

    @Schema(description = "排序")
    private Integer sort;
}
