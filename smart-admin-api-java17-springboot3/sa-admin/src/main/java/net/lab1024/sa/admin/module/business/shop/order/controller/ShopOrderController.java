package net.lab1024.sa.admin.module.business.shop.order.controller;

import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import net.lab1024.sa.admin.constant.AdminSwaggerTagConst;
import net.lab1024.sa.admin.module.business.shop.order.domain.form.ShopOrderCancelForm;
import net.lab1024.sa.admin.module.business.shop.order.domain.form.ShopOrderQueryForm;
import net.lab1024.sa.admin.module.business.shop.order.domain.form.ShopOrderRemarkForm;
import net.lab1024.sa.admin.module.business.shop.order.domain.vo.ShopOrderVO;
import net.lab1024.sa.admin.module.business.shop.order.service.ShopOrderService;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartRequestUtil;
import org.springframework.web.bind.annotation.*;

/**
 * Shop order.
 */
@RestController
@Tag(name = AdminSwaggerTagConst.Business.SHOP_ORDER)
public class ShopOrderController {

    @Resource
    private ShopOrderService shopOrderService;

    @Operation(summary = "分页查询订单")
    @PostMapping("/shop/order/queryPage")
    @SaCheckPermission("shop:order:query")
    public ResponseDTO<PageResult<ShopOrderVO>> queryPage(@RequestBody @Valid ShopOrderQueryForm queryForm) {
        return shopOrderService.queryPage(queryForm);
    }

    @Operation(summary = "查询订单详情")
    @GetMapping("/shop/order/detail/{orderId}")
    @SaCheckPermission("shop:order:detail")
    public ResponseDTO<ShopOrderVO> detail(@PathVariable Long orderId) {
        return shopOrderService.detail(orderId);
    }

    @Operation(summary = "更新订单备注")
    @PostMapping("/shop/order/updateRemark")
    @SaCheckPermission("shop:order:remark")
    public ResponseDTO<String> updateRemark(@RequestBody @Valid ShopOrderRemarkForm remarkForm) {
        return shopOrderService.updateRemark(remarkForm, SmartRequestUtil.getRequestUser());
    }

    @Operation(summary = "取消订单")
    @PostMapping("/shop/order/cancel")
    @SaCheckPermission("shop:order:cancel")
    public ResponseDTO<String> cancel(@RequestBody @Valid ShopOrderCancelForm cancelForm) {
        return shopOrderService.cancel(cancelForm, SmartRequestUtil.getRequestUser());
    }
}
