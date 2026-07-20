package net.lab1024.sa.admin.module.business.shop.customer.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerLoginForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerQueryForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerRegisterForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerRemarkForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerUpdateDisabledForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.vo.ShopCustomerLoginVO;
import net.lab1024.sa.admin.module.business.shop.customer.domain.vo.ShopCustomerVO;
import net.lab1024.sa.admin.module.business.shop.customer.service.ShopCustomerService;
import net.lab1024.sa.base.common.annoation.NoNeedLogin;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

/**
 * Shop customer.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_CUSTOMER)
public class ShopCustomerController {

    @Resource
    private ShopCustomerService shopCustomerService;

    @Operation(summary = "分页查询客户")
    @PostMapping("/shop/customer/queryPage")
    @SaCheckPermission("shop:customer:query")
    public ResponseDTO<PageResult<ShopCustomerVO>> queryPage(@RequestBody @Valid ShopCustomerQueryForm queryForm) {
        return shopCustomerService.queryPage(queryForm);
    }

    @Operation(summary = "查询客户详情")
    @GetMapping("/shop/customer/detail/{customerId}")
    @SaCheckPermission("shop:customer:detail")
    public ResponseDTO<ShopCustomerVO> detail(@PathVariable Long customerId) {
        return shopCustomerService.detail(customerId);
    }

    @Operation(summary = "更新客户禁用状态")
    @PostMapping("/shop/customer/updateDisabled")
    @SaCheckPermission("shop:customer:update")
    public ResponseDTO<String> updateDisabled(@RequestBody @Valid ShopCustomerUpdateDisabledForm updateForm) {
        return shopCustomerService.updateDisabled(updateForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "更新客户备注")
    @PostMapping("/shop/customer/updateRemark")
    @SaCheckPermission("shop:customer:remark")
    public ResponseDTO<String> updateRemark(@RequestBody @Valid ShopCustomerRemarkForm remarkForm) {
        return shopCustomerService.updateRemark(remarkForm, SmartRequestUtil.getRequestUser());
    }

    @NoNeedLogin
    @Operation(summary = "C端客户注册")
    @PostMapping("/shop/client/customer/register")
    public ResponseDTO<ShopCustomerLoginVO> register(@RequestBody @Valid ShopCustomerRegisterForm registerForm) {
        return shopCustomerService.register(registerForm);
    }

    @NoNeedLogin
    @Operation(summary = "C端客户登录")
    @PostMapping("/shop/client/customer/login")
    public ResponseDTO<ShopCustomerLoginVO> login(@RequestBody @Valid ShopCustomerLoginForm loginForm) {
        return shopCustomerService.login(loginForm);
    }
}
