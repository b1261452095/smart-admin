package net.lab1024.sa.admin.module.business.shop.setting.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.setting.domain.form.ShopSettingUpdateForm;
import net.lab1024.sa.admin.module.business.shop.setting.domain.vo.ShopSettingVO;
import net.lab1024.sa.admin.module.business.shop.setting.service.ShopSettingService;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Shop setting.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_SETTING)
public class ShopSettingController {

    @Resource
    private ShopSettingService shopSettingService;

    @Operation(summary = "获取店铺设置")
    @GetMapping("/shop/setting/get")
    @SaCheckPermission("shop:setting:query")
    public ResponseDTO<ShopSettingVO> get() {
        return shopSettingService.get();
    }

    @Operation(summary = "更新店铺设置")
    @PostMapping("/shop/setting/update")
    @SaCheckPermission("shop:setting:update")
    public ResponseDTO<String> update(@RequestBody @Valid ShopSettingUpdateForm updateForm) {
        return shopSettingService.update(updateForm, SmartRequestUtil.getRequestUser());
    }
}
