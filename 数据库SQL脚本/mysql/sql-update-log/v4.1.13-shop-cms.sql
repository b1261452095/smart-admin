-- 商城店铺装修 CMS：配置式区块

CREATE TABLE IF NOT EXISTS `shop_cms_block` (
  `block_id` bigint NOT NULL AUTO_INCREMENT COMMENT '区块ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `block_type` int NOT NULL COMMENT '区块类型：1首页Banner 2导航菜单 3推荐商品',
  `block_name` varchar(100) NOT NULL COMMENT '区块名称',
  `block_title` varchar(150) DEFAULT NULL COMMENT '展示标题',
  `block_sub_title` varchar(300) DEFAULT NULL COMMENT '展示副标题',
  `image` varchar(255) DEFAULT NULL COMMENT '图片文件key',
  `link_url` varchar(500) DEFAULT NULL COMMENT '跳转链接',
  `product_id` bigint DEFAULT NULL COMMENT '推荐商品ID',
  `product_name` varchar(200) DEFAULT NULL COMMENT '推荐商品名称',
  `config_json` text DEFAULT NULL COMMENT '扩展配置JSON',
  `sort` int NOT NULL DEFAULT 0 COMMENT '排序',
  `disabled_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '禁用状态',
  `deleted_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除状态',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `update_user_id` bigint DEFAULT NULL COMMENT '更新人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` int NOT NULL DEFAULT 1 COMMENT '版本',
  PRIMARY KEY (`block_id`),
  KEY `idx_tenant_type_sort` (`tenant_id`, `block_type`, `sort`),
  KEY `idx_product` (`product_id`),
  KEY `idx_deleted` (`deleted_flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='商城CMS区块';
