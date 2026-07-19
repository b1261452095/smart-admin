package net.lab1024.sa.admin.module.business.shop.category.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryAddForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryTreeQueryForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryUpdateDisabledForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.form.ShopCategoryUpdateForm;
import net.lab1024.sa.admin.module.business.shop.category.domain.vo.ShopCategoryVO;
import net.lab1024.sa.admin.module.business.shop.category.service.ShopCategoryService;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Shop category.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_CATEGORY)
public class ShopCategoryController {

    @Resource
    private ShopCategoryService shopCategoryService;

    @Operation(summary = "查询商城类目树")
    @PostMapping("/shop/category/tree")
    @SaCheckPermission("shop:category:query")
    public ResponseDTO<List<ShopCategoryVO>> queryTree(@RequestBody @Valid ShopCategoryTreeQueryForm queryForm) {
        return shopCategoryService.queryTree(queryForm);
    }

    @Operation(summary = "查询商城类目详情")
    @GetMapping("/shop/category/get/{categoryId}")
    @SaCheckPermission("shop:category:query")
    public ResponseDTO<ShopCategoryVO> get(@PathVariable Long categoryId) {
        return shopCategoryService.get(categoryId);
    }

    @Operation(summary = "新增商城类目")
    @PostMapping("/shop/category/add")
    @SaCheckPermission("shop:category:add")
    public ResponseDTO<String> add(@RequestBody @Valid ShopCategoryAddForm addForm) {
        return shopCategoryService.add(addForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "更新商城类目")
    @PostMapping("/shop/category/update")
    @SaCheckPermission("shop:category:update")
    public ResponseDTO<String> update(@RequestBody @Valid ShopCategoryUpdateForm updateForm) {
        return shopCategoryService.update(updateForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "启用禁用商城类目")
    @PostMapping("/shop/category/updateDisabled")
    @SaCheckPermission("shop:category:updateDisabled")
    public ResponseDTO<String> updateDisabled(@RequestBody @Valid ShopCategoryUpdateDisabledForm updateForm) {
        return shopCategoryService.updateDisabled(updateForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "删除商城类目")
    @GetMapping("/shop/category/delete/{categoryId}")
    @SaCheckPermission("shop:category:delete")
    public ResponseDTO<String> delete(@PathVariable Long categoryId) {
        return shopCategoryService.delete(categoryId, SmartRequestUtil.getRequestUser());
    }
}
