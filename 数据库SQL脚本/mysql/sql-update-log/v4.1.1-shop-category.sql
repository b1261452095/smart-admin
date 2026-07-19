-- SmartAdmin shop phase 2: shop category.
-- This script uses auto-increment menu IDs to avoid conflicts in existing databases.

CREATE TABLE IF NOT EXISTS `shop_category` (
  `category_id` bigint NOT NULL AUTO_INCREMENT COMMENT '类目ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `parent_id` bigint NOT NULL DEFAULT 0 COMMENT '父级类目ID',
  `category_name` varchar(100) NOT NULL COMMENT '类目名称',
  `category_code` varchar(100) DEFAULT NULL COMMENT '类目编码',
  `category_image` varchar(500) DEFAULT NULL COMMENT '类目图片文件key',
  `sort` int NOT NULL DEFAULT 0 COMMENT '排序',
  `disabled_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '禁用状态',
  `deleted_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标识',
  `remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `update_user_id` bigint DEFAULT NULL COMMENT '更新人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` int NOT NULL DEFAULT 0 COMMENT '乐观锁版本',
  PRIMARY KEY (`category_id`),
  UNIQUE KEY `uk_tenant_parent_name_deleted` (`tenant_id`, `parent_id`, `category_name`, `deleted_flag`),
  KEY `idx_tenant_parent_sort` (`tenant_id`, `parent_id`, `sort`),
  KEY `idx_tenant_deleted` (`tenant_id`, `deleted_flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='商城-商品类目';

SET @shop_menu_id := (
  SELECT menu_id
  FROM `t_menu`
  WHERE menu_name = '商城管理'
    AND parent_id = 0
    AND deleted_flag = 0
  ORDER BY menu_id DESC
  LIMIT 1
);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '商城管理', 1, 0, 2, '/shop', NULL, NULL, NULL, NULL, 'ShopOutlined', NULL, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE @shop_menu_id IS NULL;

SET @shop_menu_id := (
  SELECT menu_id
  FROM `t_menu`
  WHERE menu_name = '商城管理'
    AND parent_id = 0
    AND deleted_flag = 0
  ORDER BY menu_id DESC
  LIMIT 1
);

SET @shop_category_menu_id := (
  SELECT menu_id
  FROM `t_menu`
  WHERE menu_name = '类目管理'
    AND parent_id = @shop_menu_id
    AND deleted_flag = 0
  ORDER BY menu_id DESC
  LIMIT 1
);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '类目管理', 2, @shop_menu_id, 2, '/shop/category', '/business/shop/category/shop-category.vue', NULL, NULL, NULL, 'ApartmentOutlined', NULL, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE @shop_category_menu_id IS NULL;

SET @shop_category_menu_id := (
  SELECT menu_id
  FROM `t_menu`
  WHERE menu_name = '类目管理'
    AND parent_id = @shop_menu_id
    AND deleted_flag = 0
  ORDER BY menu_id DESC
  LIMIT 1
);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '查询', 3, @shop_category_menu_id, 1, NULL, NULL, 1, 'shop:category:query', 'shop:category:query', NULL, @shop_category_menu_id, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE parent_id = @shop_category_menu_id AND web_perms = 'shop:category:query' AND deleted_flag = 0);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '新增', 3, @shop_category_menu_id, 2, NULL, NULL, 1, 'shop:category:add', 'shop:category:add', NULL, @shop_category_menu_id, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE parent_id = @shop_category_menu_id AND web_perms = 'shop:category:add' AND deleted_flag = 0);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '编辑', 3, @shop_category_menu_id, 3, NULL, NULL, 1, 'shop:category:update', 'shop:category:update', NULL, @shop_category_menu_id, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE parent_id = @shop_category_menu_id AND web_perms = 'shop:category:update' AND deleted_flag = 0);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '启用/禁用', 3, @shop_category_menu_id, 4, NULL, NULL, 1, 'shop:category:updateDisabled', 'shop:category:updateDisabled', NULL, @shop_category_menu_id, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE parent_id = @shop_category_menu_id AND web_perms = 'shop:category:updateDisabled' AND deleted_flag = 0);

INSERT INTO `t_menu`
  (`menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT '删除', 3, @shop_category_menu_id, 5, NULL, NULL, 1, 'shop:category:delete', 'shop:category:delete', NULL, @shop_category_menu_id, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE parent_id = @shop_category_menu_id AND web_perms = 'shop:category:delete' AND deleted_flag = 0);

-- Grant the shop category menu and its permission points to every existing role.
INSERT INTO `t_role_menu` (`role_id`, `menu_id`, `create_time`, `update_time`)
SELECT r.role_id, m.menu_id, NOW(), NOW()
FROM `t_role` r
JOIN `t_menu` m ON m.menu_id = @shop_menu_id
WHERE NOT EXISTS (
  SELECT 1
  FROM `t_role_menu` rm
  WHERE rm.role_id = r.role_id
    AND rm.menu_id = m.menu_id
);

INSERT INTO `t_role_menu` (`role_id`, `menu_id`, `create_time`, `update_time`)
SELECT r.role_id, m.menu_id, NOW(), NOW()
FROM `t_role` r
JOIN `t_menu` m ON m.menu_id = @shop_category_menu_id OR m.parent_id = @shop_category_menu_id
WHERE m.deleted_flag = 0
  AND NOT EXISTS (
    SELECT 1
    FROM `t_role_menu` rm
    WHERE rm.role_id = r.role_id
      AND rm.menu_id = m.menu_id
  );
