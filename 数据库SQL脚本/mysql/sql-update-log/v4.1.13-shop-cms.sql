-- v4.1.13 商城店铺装修 CMS（配置式区块 + 拖拽装修）
-- 适用：MySQL 5.7 / 8.0
-- 说明：
-- 1. 此脚本只负责数据表和首页区块初始化，不包含 t_menu 菜单数据。
-- 2. image 字段保存 SmartAdmin 文件上传后的 fileKey，因此初始化数据不预填图片。
-- 3. 执行后请在“商城管理 -> 店铺装修”中上传主视觉和区块图片。

CREATE TABLE IF NOT EXISTS `shop_cms_block` (
  `block_id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '区块ID',
  `tenant_id` BIGINT NOT NULL DEFAULT 1 COMMENT '租户ID',
  `block_type` TINYINT NOT NULL COMMENT '区块类型：1主视觉 2分类入口 3单品推荐 4商品列表 5图文双栏 6全幅图片 7公告栏 8视频',
  `block_name` VARCHAR(100) NOT NULL COMMENT '后台内部名称',
  `block_title` VARCHAR(150) DEFAULT NULL COMMENT '展示标题',
  `block_sub_title` VARCHAR(300) DEFAULT NULL COMMENT '展示副标题',
  `image` VARCHAR(1000) DEFAULT NULL COMMENT '图片文件key，多个用逗号分隔',
  `link_url` VARCHAR(500) DEFAULT NULL COMMENT '跳转链接',
  `product_id` BIGINT DEFAULT NULL COMMENT '推荐商品ID',
  `product_name` VARCHAR(200) DEFAULT NULL COMMENT '推荐商品名称快照',
  `config_json` TEXT DEFAULT NULL COMMENT '区块扩展配置JSON',
  `sort` INT NOT NULL DEFAULT 0 COMMENT '页面排序',
  `disabled_flag` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否隐藏：0否 1是',
  `deleted_flag` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否删除：0否 1是',
  `create_user_id` BIGINT DEFAULT NULL COMMENT '创建人',
  `update_user_id` BIGINT DEFAULT NULL COMMENT '更新人',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `version` INT NOT NULL DEFAULT 0 COMMENT '乐观锁版本',
  PRIMARY KEY (`block_id`),
  KEY `idx_cms_tenant_status_sort` (`tenant_id`, `deleted_flag`, `disabled_flag`, `sort`),
  KEY `idx_cms_product_id` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='店铺装修CMS区块';

-- 兼容已经创建过的旧表：按需补 config_json。
SET @cms_has_config_json = (
  SELECT COUNT(*)
  FROM `information_schema`.`COLUMNS`
  WHERE `TABLE_SCHEMA` = DATABASE()
    AND `TABLE_NAME` = 'shop_cms_block'
    AND `COLUMN_NAME` = 'config_json'
);
SET @cms_config_json_sql = IF(
  @cms_has_config_json = 0,
  'ALTER TABLE `shop_cms_block` ADD COLUMN `config_json` TEXT DEFAULT NULL COMMENT ''区块扩展配置JSON'' AFTER `product_name`',
  'SELECT 1'
);
PREPARE cms_config_json_stmt FROM @cms_config_json_sql;
EXECUTE cms_config_json_stmt;
DEALLOCATE PREPARE cms_config_json_stmt;

-- 兼容已经创建过的旧表：按需补 version。
SET @cms_has_version = (
  SELECT COUNT(*)
  FROM `information_schema`.`COLUMNS`
  WHERE `TABLE_SCHEMA` = DATABASE()
    AND `TABLE_NAME` = 'shop_cms_block'
    AND `COLUMN_NAME` = 'version'
);
SET @cms_version_sql = IF(
  @cms_has_version = 0,
  'ALTER TABLE `shop_cms_block` ADD COLUMN `version` INT NOT NULL DEFAULT 0 COMMENT ''乐观锁版本'' AFTER `update_time`',
  'SELECT 1'
);
PREPARE cms_version_stmt FROM @cms_version_sql;
EXECUTE cms_version_stmt;
DEALLOCATE PREPARE cms_version_stmt;

-- 首页初始化结构。脚本可重复执行，以 tenant_id + block_name 防重。
INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 7, 'store-announcement', 'Jewelry and intimates, considered together', NULL, NULL, '/search', NULL, NULL,
  '{"theme":"dark"}', 10, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'store-announcement' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 1, 'home-hero', 'Made For Your Own Rhythm',
  'New intimates, sculptural jewelry, and pieces that move easily between private and public.',
  NULL, '/collections/intimates', NULL, NULL,
  '{"height":"tall","textPosition":"left","buttonText":"Shop new arrivals"}',
  20, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'home-hero' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 4, 'new-and-noted', 'New and Noted',
  'The latest pieces across intimates and jewelry.',
  NULL, '/search', NULL, NULL,
  '{"categorySlug":"","limit":8,"columns":4,"collectionUrl":"/search"}',
  30, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'new-and-noted' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 2, 'category-intimates', 'Intimates', 'Satin, lace, and first layers.',
  NULL, '/collections/intimates', NULL, NULL, '{"layout":"grid"}',
  40, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'category-intimates' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 2, 'category-jewelry', 'Jewelry', 'Light-catching pieces for every day.',
  NULL, '/search?keyword=jewelry', NULL, NULL, '{"layout":"grid"}',
  50, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'category-jewelry' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 2, 'category-sleepwear', 'Sleepwear', 'Soft tailoring after dark.',
  NULL, '/collections/sleepwear', NULL, NULL, '{"layout":"grid"}',
  60, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'category-sleepwear' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 5, 'first-layer-story', 'The First Layer',
  'Quietly expressive pieces designed to be worn your way, from the first layer outward.',
  NULL, '/collections/intimates', NULL, NULL,
  '{"imagePosition":"right","buttonText":"Discover the edit"}',
  70, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'first-layer-story' AND `deleted_flag` = 0
);

INSERT INTO `shop_cms_block`
  (`tenant_id`, `block_type`, `block_name`, `block_title`, `block_sub_title`, `image`, `link_url`, `product_id`, `product_name`, `config_json`, `sort`, `disabled_flag`, `deleted_flag`, `create_user_id`, `update_user_id`, `create_time`, `update_time`, `version`)
SELECT
  1, 6, 'evening-edit', 'The Evening Edit',
  'Satin, silver, and a little more intention.',
  NULL, '/search', NULL, NULL,
  '{"height":"tall","buttonText":"Explore the edit"}',
  80, 0, 0, 1, 1, NOW(), NOW(), 0
WHERE NOT EXISTS (
  SELECT 1 FROM `shop_cms_block`
  WHERE `tenant_id` = 1 AND `block_name` = 'evening-edit' AND `deleted_flag` = 0
);

SELECT
  `block_id`,
  `block_type`,
  `block_name`,
  `block_title`,
  `sort`,
  `disabled_flag`,
  `version`
FROM `shop_cms_block`
WHERE `tenant_id` = 1
  AND `deleted_flag` = 0
ORDER BY `sort`, `block_id`;
