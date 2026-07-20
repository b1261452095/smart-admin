-- 商城阶段5：库存流水
-- 前置依赖：v4.1.3-shop-sku.sql 已创建 shop_inventory

CREATE TABLE IF NOT EXISTS `shop_inventory_record` (
  `record_id` bigint NOT NULL AUTO_INCREMENT COMMENT '库存流水ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `product_id` bigint NOT NULL COMMENT '商品ID',
  `sku_id` bigint NOT NULL COMMENT 'SKU ID',
  `operation_type` int NOT NULL DEFAULT 1 COMMENT '操作类型：1手动调整 2订单锁定 3订单释放 4订单扣减',
  `change_quantity` int NOT NULL DEFAULT 0 COMMENT '变动数量',
  `before_available_stock` int NOT NULL DEFAULT 0 COMMENT '变动前可售库存',
  `after_available_stock` int NOT NULL DEFAULT 0 COMMENT '变动后可售库存',
  `before_locked_stock` int NOT NULL DEFAULT 0 COMMENT '变动前锁定库存',
  `after_locked_stock` int NOT NULL DEFAULT 0 COMMENT '变动后锁定库存',
  `before_sold_stock` int NOT NULL DEFAULT 0 COMMENT '变动前已售库存',
  `after_sold_stock` int NOT NULL DEFAULT 0 COMMENT '变动后已售库存',
  `remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`record_id`),
  KEY `idx_sku_record` (`sku_id`, `record_id`),
  KEY `idx_product_record` (`product_id`, `record_id`),
  KEY `idx_tenant_record` (`tenant_id`, `record_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商城库存流水';
