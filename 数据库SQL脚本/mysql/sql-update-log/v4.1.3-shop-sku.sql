-- 商城阶段4：商品SKU + 当前库存

CREATE TABLE IF NOT EXISTS `shop_product_sku` (
  `sku_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'SKU ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `product_id` bigint NOT NULL COMMENT '商品ID',
  `sku_name` varchar(200) NOT NULL COMMENT 'SKU名称',
  `sku_code` varchar(100) DEFAULT NULL COMMENT 'SKU编码',
  `spec_json` text COMMENT '规格JSON',
  `spec_summary` varchar(500) DEFAULT NULL COMMENT '规格摘要',
  `sku_image` varchar(500) DEFAULT NULL COMMENT 'SKU图片',
  `sale_price_cent` bigint NOT NULL DEFAULT 0 COMMENT '售价，单位分',
  `market_price_cent` bigint DEFAULT NULL COMMENT '市场价，单位分',
  `cost_price_cent` bigint DEFAULT NULL COMMENT '成本价，单位分',
  `currency` varchar(20) NOT NULL DEFAULT 'USD' COMMENT '币种',
  `disabled_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '禁用状态',
  `deleted_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除状态',
  `sort` int NOT NULL DEFAULT 0 COMMENT '排序',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `update_user_id` bigint DEFAULT NULL COMMENT '更新人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` int NOT NULL DEFAULT 0 COMMENT '版本号',
  PRIMARY KEY (`sku_id`),
  UNIQUE KEY `uk_tenant_product_sku_code_deleted` (`tenant_id`, `product_id`, `sku_code`, `deleted_flag`),
  KEY `idx_product_deleted` (`product_id`, `deleted_flag`),
  KEY `idx_tenant_product` (`tenant_id`, `product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商城商品SKU';

CREATE TABLE IF NOT EXISTS `shop_inventory` (
  `inventory_id` bigint NOT NULL AUTO_INCREMENT COMMENT '库存ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `product_id` bigint NOT NULL COMMENT '商品ID',
  `sku_id` bigint NOT NULL COMMENT 'SKU ID',
  `available_stock` int NOT NULL DEFAULT 0 COMMENT '可售库存',
  `locked_stock` int NOT NULL DEFAULT 0 COMMENT '锁定库存',
  `sold_stock` int NOT NULL DEFAULT 0 COMMENT '已售库存',
  `warning_stock` int NOT NULL DEFAULT 0 COMMENT '预警库存',
  `deleted_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除状态',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `update_user_id` bigint DEFAULT NULL COMMENT '更新人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` int NOT NULL DEFAULT 0 COMMENT '版本号',
  PRIMARY KEY (`inventory_id`),
  UNIQUE KEY `uk_sku_deleted` (`sku_id`, `deleted_flag`),
  KEY `idx_product_deleted` (`product_id`, `deleted_flag`),
  KEY `idx_tenant_product` (`tenant_id`, `product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商城SKU当前库存';
