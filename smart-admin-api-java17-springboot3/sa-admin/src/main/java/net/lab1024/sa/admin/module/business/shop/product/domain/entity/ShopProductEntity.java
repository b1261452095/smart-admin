package net.lab1024.sa.admin.module.business.shop.product.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop product entity.
 */
@Data
@TableName("shop_product")
public class ShopProductEntity {

    @TableId(type = IdType.AUTO)
    private Long productId;

    private Long tenantId;

    private Long categoryId;

    private String productName;

    private String productCode;

    private String subTitle;

    private String mainImage;

    private String detailImages;

    private Long salePriceCent;

    private String currency;

    private Integer publishStatus;

    private Boolean shelvesFlag;

    private Integer sort;

    private String seoTitle;

    private String seoDescription;

    private String productDetail;

    private String remark;

    private Boolean deletedFlag;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
