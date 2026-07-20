package net.lab1024.sa.admin.module.business.shop.cms.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Shop CMS block entity.
 */
@Data
@TableName("shop_cms_block")
public class ShopCmsBlockEntity {

    @TableId(type = IdType.AUTO)
    private Long blockId;

    private Long tenantId;

    private Integer blockType;

    private String blockName;

    private String blockTitle;

    private String blockSubTitle;

    private String image;

    private String linkUrl;

    private Long productId;

    private String productName;

    private String configJson;

    private Integer sort;

    private Boolean disabledFlag;

    private Boolean deletedFlag;

    private Long createUserId;

    private Long updateUserId;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private Integer version;
}
