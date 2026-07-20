package net.lab1024.sa.admin.module.business.shop.inventory.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.form.ShopInventoryAdjustForm;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.form.ShopInventoryQueryForm;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.form.ShopInventoryRecordQueryForm;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.vo.ShopInventoryRecordVO;
import net.lab1024.sa.admin.module.business.shop.inventory.domain.vo.ShopInventoryVO;
import net.lab1024.sa.admin.module.business.shop.inventory.service.ShopInventoryService;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

/**
 * Shop inventory.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_INVENTORY)
public class ShopInventoryController {

    @Resource
    private ShopInventoryService shopInventoryService;

    @Operation(summary = "分页查询库存")
    @PostMapping("/shop/inventory/queryPage")
    @SaCheckPermission("shop:inventory:query")
    public ResponseDTO<PageResult<ShopInventoryVO>> queryPage(@RequestBody @Valid ShopInventoryQueryForm queryForm) {
        return shopInventoryService.queryPage(queryForm);
    }

    @Operation(summary = "调整库存")
    @PostMapping("/shop/inventory/adjust")
    @SaCheckPermission("shop:inventory:adjust")
    public ResponseDTO<String> adjust(@RequestBody @Valid ShopInventoryAdjustForm adjustForm) {
        return shopInventoryService.adjust(adjustForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "分页查询库存流水")
    @PostMapping("/shop/inventory/record/queryPage")
    @SaCheckPermission("shop:inventory:record:query")
    public ResponseDTO<PageResult<ShopInventoryRecordVO>> queryRecordPage(@RequestBody @Valid ShopInventoryRecordQueryForm queryForm) {
        return shopInventoryService.queryRecordPage(queryForm);
    }
}
