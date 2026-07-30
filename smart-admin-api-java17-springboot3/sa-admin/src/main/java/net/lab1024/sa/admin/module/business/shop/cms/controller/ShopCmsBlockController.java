package net.lab1024.sa.admin.module.business.shop.cms.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockAddForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockClientQueryForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockDisabledForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockQueryForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockSortForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.form.ShopCmsBlockUpdateForm;
import net.lab1024.sa.admin.module.business.shop.cms.domain.vo.ShopCmsBlockVO;
import net.lab1024.sa.admin.module.business.shop.cms.service.ShopCmsBlockService;
import net.lab1024.sa.base.common.annoation.NoNeedLogin;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

/**
 * Shop CMS block.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_CMS)
public class ShopCmsBlockController {

    @Resource
    private ShopCmsBlockService shopCmsBlockService;

    @Operation(summary = "分页查询CMS区块")
    @PostMapping("/shop/cms/block/queryPage")
    @SaCheckPermission("shop:cms:query")
    public ResponseDTO<PageResult<ShopCmsBlockVO>> queryPage(@RequestBody @Valid ShopCmsBlockQueryForm queryForm) {
        return shopCmsBlockService.queryPage(queryForm);
    }

    @Operation(summary = "查询店铺装修区块列表")
    @PostMapping("/shop/cms/block/queryList")
    @SaCheckPermission("shop:cms:query")
    public ResponseDTO<java.util.List<ShopCmsBlockVO>> queryList(@RequestBody @Valid ShopCmsBlockQueryForm queryForm) {
        return shopCmsBlockService.queryList(queryForm);
    }

    @Operation(summary = "查询CMS区块详情")
    @GetMapping("/shop/cms/block/get/{blockId}")
    @SaCheckPermission("shop:cms:query")
    public ResponseDTO<ShopCmsBlockVO> get(@PathVariable Long blockId) {
        return shopCmsBlockService.get(blockId);
    }

    @NoNeedLogin
    @Operation(summary = "C端查询启用CMS区块")
    @PostMapping("/shop/client/cms/block/list")
    public ResponseDTO<java.util.List<ShopCmsBlockVO>> queryClientList(@RequestBody ShopCmsBlockClientQueryForm queryForm) {
        return shopCmsBlockService.queryClientList(queryForm);
    }

    @Operation(summary = "新增CMS区块")
    @PostMapping("/shop/cms/block/add")
    @SaCheckPermission("shop:cms:add")
    public ResponseDTO<String> add(@RequestBody @Valid ShopCmsBlockAddForm addForm) {
        return shopCmsBlockService.add(addForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "更新CMS区块")
    @PostMapping("/shop/cms/block/update")
    @SaCheckPermission("shop:cms:update")
    public ResponseDTO<String> update(@RequestBody @Valid ShopCmsBlockUpdateForm updateForm) {
        return shopCmsBlockService.update(updateForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "更新CMS区块禁用状态")
    @PostMapping("/shop/cms/block/updateDisabled")
    @SaCheckPermission("shop:cms:update")
    public ResponseDTO<String> updateDisabled(@RequestBody @Valid ShopCmsBlockDisabledForm disabledForm) {
        return shopCmsBlockService.updateDisabled(disabledForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "批量更新CMS区块顺序")
    @PostMapping("/shop/cms/block/updateSort")
    @SaCheckPermission("shop:cms:update")
    public ResponseDTO<String> updateSort(@RequestBody @Valid ShopCmsBlockSortForm sortForm) {
        return shopCmsBlockService.updateSort(sortForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "删除CMS区块")
    @GetMapping("/shop/cms/block/delete/{blockId}")
    @SaCheckPermission("shop:cms:delete")
    public ResponseDTO<String> delete(@PathVariable Long blockId) {
        return shopCmsBlockService.delete(blockId, SmartRequestUtil.getRequestUser());
    }
}
