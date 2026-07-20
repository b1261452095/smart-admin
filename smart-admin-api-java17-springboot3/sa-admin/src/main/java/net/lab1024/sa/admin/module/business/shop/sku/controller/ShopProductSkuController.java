package net.lab1024.sa.admin.module.business.shop.sku.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuBatchSaveForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuDisabledForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuQueryForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.form.ShopProductSkuSaveForm;
import net.lab1024.sa.admin.module.business.shop.sku.domain.vo.ShopProductSkuVO;
import net.lab1024.sa.admin.module.business.shop.sku.service.ShopProductSkuService;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Shop product SKU.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_SKU)
public class ShopProductSkuController {

    @Resource
    private ShopProductSkuService shopProductSkuService;

    @Operation(summary = "查询商品SKU列表")
    @PostMapping("/shop/product/sku/queryList")
    @SaCheckPermission("shop:sku:query")
    public ResponseDTO<List<ShopProductSkuVO>> queryList(@RequestBody @Valid ShopProductSkuQueryForm queryForm) {
        return shopProductSkuService.queryList(queryForm.getProductId());
    }

    @Operation(summary = "保存商品SKU")
    @PostMapping("/shop/product/sku/save")
    @SaCheckPermission("shop:sku:update")
    public ResponseDTO<String> save(@RequestBody @Valid ShopProductSkuSaveForm saveForm) {
        return shopProductSkuService.save(saveForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "批量保存商品SKU")
    @PostMapping("/shop/product/sku/saveList")
    @SaCheckPermission("shop:sku:update")
    public ResponseDTO<String> saveList(@RequestBody @Valid ShopProductSkuBatchSaveForm saveForm) {
        return shopProductSkuService.saveList(saveForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "启用禁用商品SKU")
    @PostMapping("/shop/product/sku/updateDisabled")
    @SaCheckPermission("shop:sku:update")
    public ResponseDTO<String> updateDisabled(@RequestBody @Valid ShopProductSkuDisabledForm disabledForm) {
        return shopProductSkuService.updateDisabled(disabledForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "删除商品SKU")
    @GetMapping("/shop/product/sku/delete/{skuId}")
    @SaCheckPermission("shop:sku:update")
    public ResponseDTO<String> delete(@PathVariable Long skuId) {
        return shopProductSkuService.delete(skuId, SmartRequestUtil.getRequestUser());
    }
}
