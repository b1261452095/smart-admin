-- SmartAdmin shop phase 1: menu and shop setting.

CREATE TABLE IF NOT EXISTS `shop_setting` (
  `setting_id` bigint NOT NULL AUTO_INCREMENT COMMENT '设置ID',
  `tenant_id` bigint NOT NULL DEFAULT 1 COMMENT '租户ID',
  `store_name` varchar(100) NOT NULL COMMENT '店铺名称',
  `store_logo` varchar(500) DEFAULT NULL COMMENT '店铺Logo文件key',
  `store_domain` varchar(200) DEFAULT NULL COMMENT '店铺域名',
  `default_language` varchar(20) NOT NULL DEFAULT 'zh-CN' COMMENT '默认语言',
  `default_currency` varchar(20) NOT NULL DEFAULT 'USD' COMMENT '默认币种',
  `support_email` varchar(100) DEFAULT NULL COMMENT '客服邮箱',
  `tax_enabled_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否启用税费',
  `checkout_enabled_flag` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否允许结账',
  `maintenance_enabled_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否维护中',
  `seo_title` varchar(200) DEFAULT NULL COMMENT 'SEO标题',
  `seo_description` varchar(500) DEFAULT NULL COMMENT 'SEO描述',
  `remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `deleted_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标识',
  `create_user_id` bigint DEFAULT NULL COMMENT '创建人',
  `update_user_id` bigint DEFAULT NULL COMMENT '更新人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` int NOT NULL DEFAULT 0 COMMENT '乐观锁版本',
  PRIMARY KEY (`setting_id`),
  UNIQUE KEY `uk_tenant` (`tenant_id`),
  KEY `idx_tenant_deleted` (`tenant_id`, `deleted_flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='商城-店铺设置';

INSERT INTO `shop_setting`
  (`tenant_id`, `store_name`, `default_language`, `default_currency`, `tax_enabled_flag`, `checkout_enabled_flag`, `maintenance_enabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`)
SELECT 1, 'Smart Shop', 'zh-CN', 'USD', 0, 1, 0, 0, 1, 1
WHERE NOT EXISTS (SELECT 1 FROM `shop_setting` WHERE `tenant_id` = 1);

INSERT INTO `t_menu`
  (`menu_id`, `menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT 300, '商城管理', 1, 0, 2, '/shop', NULL, NULL, NULL, NULL, 'ShopOutlined', NULL, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE `menu_id` = 300);

INSERT INTO `t_menu`
  (`menu_id`, `menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT 301, '店铺设置', 2, 300, 1, '/shop/setting', '/business/shop/setting/shop-setting.vue', NULL, NULL, NULL, 'SettingOutlined', NULL, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE `menu_id` = 301);

INSERT INTO `t_menu`
  (`menu_id`, `menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT 302, '查询', 3, 301, 1, NULL, NULL, 1, 'shop:setting:query', 'shop:setting:query', NULL, 301, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE `menu_id` = 302);

INSERT INTO `t_menu`
  (`menu_id`, `menu_name`, `menu_type`, `parent_id`, `sort`, `path`, `component`, `perms_type`, `api_perms`, `web_perms`, `icon`, `context_menu_id`, `frame_flag`, `frame_url`, `cache_flag`, `visible_flag`, `disabled_flag`, `deleted_flag`, `create_user_id`, `create_time`, `update_user_id`, `update_time`)
SELECT 303, '更新', 3, 301, 2, NULL, NULL, 1, 'shop:setting:update', 'shop:setting:update', NULL, 301, 0, NULL, 0, 1, 0, 0, 1, NOW(), 1, NOW()
WHERE NOT EXISTS (SELECT 1 FROM `t_menu` WHERE `menu_id` = 303);

-- Grant the new shop menu to the default admin role.
INSERT INTO `t_role_menu` (`role_id`, `menu_id`, `create_time`, `update_time`)
SELECT 1, 300, NOW(), NOW()
WHERE EXISTS (SELECT 1 FROM `t_role` WHERE `role_id` = 1)
  AND NOT EXISTS (SELECT 1 FROM `t_role_menu` WHERE `role_id` = 1 AND `menu_id` = 300);

INSERT INTO `t_role_menu` (`role_id`, `menu_id`, `create_time`, `update_time`)
SELECT 1, 301, NOW(), NOW()
WHERE EXISTS (SELECT 1 FROM `t_role` WHERE `role_id` = 1)
  AND NOT EXISTS (SELECT 1 FROM `t_role_menu` WHERE `role_id` = 1 AND `menu_id` = 301);

INSERT INTO `t_role_menu` (`role_id`, `menu_id`, `create_time`, `update_time`)
SELECT 1, 302, NOW(), NOW()
WHERE EXISTS (SELECT 1 FROM `t_role` WHERE `role_id` = 1)
  AND NOT EXISTS (SELECT 1 FROM `t_role_menu` WHERE `role_id` = 1 AND `menu_id` = 302);

INSERT INTO `t_role_menu` (`role_id`, `menu_id`, `create_time`, `update_time`)
SELECT 1, 303, NOW(), NOW()
WHERE EXISTS (SELECT 1 FROM `t_role` WHERE `role_id` = 1)
  AND NOT EXISTS (SELECT 1 FROM `t_role_menu` WHERE `role_id` = 1 AND `menu_id` = 303);
