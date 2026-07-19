package net.lab1024.sa.admin.module.business.shop.product.domain.form;

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
 * Shop product add form.
 */
@Data
public class ShopProductAddForm {

    @Schema(description = "租户ID")
    private Long tenantId;

    @Schema(description = "类目ID")
    @NotNull(message = "请选择商品类目")
    private Long categoryId;

    @Schema(description = "商品名称")
    @NotBlank(message = "商品名称不能为空")
    @Length(max = 200, message = "商品名称最多200字符")
    private String productName;

    @Schema(description = "商品编码")
    @Length(max = 100, message = "商品编码最多100字符")
    private String productCode;

    @Schema(description = "副标题")
    @Length(max = 300, message = "副标题最多300字符")
    private String subTitle;

    @Schema(description = "主图")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    @JsonDeserialize(using = FileKeyVoDeserializer.class)
    private String mainImage;

    @Schema(description = "详情图片")
    @JsonSerialize(using = FileKeyVoSerializer.class)
    @JsonDeserialize(using = FileKeyVoDeserializer.class)
    private String detailImages;

    @Schema(description = "售价，单位分")
    @NotNull(message = "售价不能为空")
    @Min(value = 0, message = "售价不能小于0")
    private Long salePriceCent;

    @Schema(description = "币种")
    @NotBlank(message = "币种不能为空")
    @Length(max = 20, message = "币种最多20字符")
    private String currency;

    @Schema(description = "发布状态 1草稿 2已发布")
    @NotNull(message = "发布状态不能为空")
    private Integer publishStatus;

    @Schema(description = "上架状态")
    @NotNull(message = "上架状态不能为空")
    private Boolean shelvesFlag;

    @Schema(description = "排序")
    private Integer sort;

    @Schema(description = "SEO标题")
    @Length(max = 200, message = "SEO标题最多200字符")
    private String seoTitle;

    @Schema(description = "SEO描述")
    @Length(max = 500, message = "SEO描述最多500字符")
    private String seoDescription;

    @Schema(description = "商品详情")
    private String productDetail;

    @Schema(description = "备注")
    @Length(max = 500, message = "备注最多500字符")
    private String remark;
}
