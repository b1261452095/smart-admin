package net.lab1024.sa.admin.module.business.shop.sku.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop product SKU entity.
 */
@Data
@TableName("shop_product_sku")
public class ShopProductSkuEntity {

    @TableId(type = IdType.AUTO)
    private Long skuId;

    private Long tenantId;

    private Long productId;

    private String skuName;

    private String skuCode;

    private String specJson;

    private String specSummary;

    private String skuImage;

    private Long salePriceCent;

    private Long marketPriceCent;

    private Long costPriceCent;

    private String currency;

    private Boolean disabledFlag;

    private Boolean deletedFlag;

    private Integer sort;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
