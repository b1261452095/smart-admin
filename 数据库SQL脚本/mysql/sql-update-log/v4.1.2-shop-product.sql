-- SmartAdmin shop phase 3: shop product SPU.
-- Menu entries are intended to be added from SmartAdmin menu UI.

CREATE TABLE IF NOT EXISTS `shop_product` (
  `product_id` bigint NOT NULL AUTO_INCREMENT COMMENT '商品ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `category_id` bigint NOT NULL COMMENT '商品类目ID',
  `product_name` varchar(200) NOT NULL COMMENT '商品名称',
  `product_code` varchar(100) DEFAULT NULL COMMENT '商品编码',
  `sub_title` varchar(300) DEFAULT NULL COMMENT '副标题',
  `main_image` varchar(500) DEFAULT NULL COMMENT '主图文件key',
  `detail_images` varchar(2000) DEFAULT NULL COMMENT '详情图片文件key，多个逗号分隔',
  `sale_price_cent` bigint NOT NULL DEFAULT 0 COMMENT '售价，单位分',
  `currency` varchar(20) NOT NULL DEFAULT 'USD' COMMENT '币种',
  `publish_status` tinyint NOT NULL DEFAULT 1 COMMENT '发布状态 1草稿 2已发布',
  `shelves_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '上架状态',
  `sort` int NOT NULL DEFAULT 0 COMMENT '排序',
  `seo_title` varchar(200) DEFAULT NULL COMMENT 'SEO标题',
  `seo_description` varchar(500) DEFAULT NULL COMMENT 'SEO描述',
  `product_detail` text COMMENT '商品详情',
  `remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `deleted_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标识',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `update_user_id` bigint DEFAULT NULL COMMENT '更新人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` int NOT NULL DEFAULT 0 COMMENT '乐观锁版本',
  PRIMARY KEY (`product_id`),
  UNIQUE KEY `uk_tenant_product_code_deleted` (`tenant_id`, `product_code`, `deleted_flag`),
  KEY `idx_tenant_category` (`tenant_id`, `category_id`),
  KEY `idx_tenant_status_shelves` (`tenant_id`, `publish_status`, `shelves_flag`),
  KEY `idx_tenant_deleted` (`tenant_id`, `deleted_flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='商城-商品SPU';
