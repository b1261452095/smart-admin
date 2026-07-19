package net.lab1024.sa.admin.module.business.shop.product.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductAddForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductQueryForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductShelvesForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.form.ShopProductUpdateForm;
import net.lab1024.sa.admin.module.business.shop.product.domain.vo.ShopProductVO;
import net.lab1024.sa.admin.module.business.shop.product.service.ShopProductService;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

/**
 * Shop product.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_PRODUCT)
public class ShopProductController {

    @Resource
    private ShopProductService shopProductService;

    @Operation(summary = "分页查询商城商品")
    @PostMapping("/shop/product/queryPage")
    @SaCheckPermission("shop:product:query")
    public ResponseDTO<PageResult<ShopProductVO>> queryPage(@RequestBody @Valid ShopProductQueryForm queryForm) {
        return shopProductService.queryPage(queryForm);
    }

    @Operation(summary = "查询商城商品详情")
    @GetMapping("/shop/product/get/{productId}")
    @SaCheckPermission("shop:product:query")
    public ResponseDTO<ShopProductVO> get(@PathVariable Long productId) {
        return shopProductService.get(productId);
    }

    @Operation(summary = "新增商城商品")
    @PostMapping("/shop/product/add")
    @SaCheckPermission("shop:product:add")
    public ResponseDTO<String> add(@RequestBody @Valid ShopProductAddForm addForm) {
        return shopProductService.add(addForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "更新商城商品")
    @PostMapping("/shop/product/update")
    @SaCheckPermission("shop:product:update")
    public ResponseDTO<String> update(@RequestBody @Valid ShopProductUpdateForm updateForm) {
        return shopProductService.update(updateForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "上下架商城商品")
    @PostMapping("/shop/product/updateShelves")
    @SaCheckPermission("shop:product:shelve")
    public ResponseDTO<String> updateShelves(@RequestBody @Valid ShopProductShelvesForm shelvesForm) {
        return shopProductService.updateShelves(shelvesForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "删除商城商品")
    @GetMapping("/shop/product/delete/{productId}")
    @SaCheckPermission("shop:product:delete")
    public ResponseDTO<String> delete(@PathVariable Long productId) {
        return shopProductService.delete(productId, SmartRequestUtil.getRequestUser());
    }
}
