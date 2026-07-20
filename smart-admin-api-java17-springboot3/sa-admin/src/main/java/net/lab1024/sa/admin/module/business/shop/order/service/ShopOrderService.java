package net.lab1024.sa.admin.module.business.shop.order.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.order.dao.ShopOrderDao;
import net.lab1024.sa.admin.module.business.shop.order.dao.ShopOrderItemDao;
import net.lab1024.sa.admin.module.business.shop.order.domain.entity.ShopOrderEntity;
import net.lab1024.sa.admin.module.business.shop.order.domain.entity.ShopOrderItemEntity;
import net.lab1024.sa.admin.module.business.shop.order.domain.form.ShopOrderCancelForm;
import net.lab1024.sa.admin.module.business.shop.order.domain.form.ShopOrderQueryForm;
import net.lab1024.sa.admin.module.business.shop.order.domain.form.ShopOrderRemarkForm;
import net.lab1024.sa.admin.module.business.shop.order.domain.vo.ShopOrderItemVO;
import net.lab1024.sa.admin.module.business.shop.order.domain.vo.ShopOrderVO;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import net.lab1024.sa.base.common.util.SmartPageUtil;
import net.lab1024.sa.base.common.util.SmartStringUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shop order service.
 */
@Service
public class ShopOrderService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    private static final Integer ORDER_STATUS_COMPLETED = 4;

    private static final Integer ORDER_STATUS_CANCELED = 5;

    private static final Integer PAY_STATUS_PAID = 2;

    @Resource
    private ShopOrderDao shopOrderDao;

    @Resource
    private ShopOrderItemDao shopOrderItemDao;

    /**
     * Query order page.
     */
    public ResponseDTO<PageResult<ShopOrderVO>> queryPage(ShopOrderQueryForm queryForm) {
        LambdaQueryWrapper<ShopOrderEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopOrderEntity::getTenantId, queryForm.getTenantId() == null ? DEFAULT_TENANT_ID : queryForm.getTenantId());
        queryWrapper.eq(ShopOrderEntity::getDeletedFlag, Boolean.FALSE);
        if (SmartStringUtil.isNotEmpty(queryForm.getOrderNo())) {
            queryWrapper.like(ShopOrderEntity::getOrderNo, queryForm.getOrderNo());
        }
        if (queryForm.getOrderStatus() != null) {
            queryWrapper.eq(ShopOrderEntity::getOrderStatus, queryForm.getOrderStatus());
        }
        if (queryForm.getPayStatus() != null) {
            queryWrapper.eq(ShopOrderEntity::getPayStatus, queryForm.getPayStatus());
        }
        if (queryForm.getFulfillmentStatus() != null) {
            queryWrapper.eq(ShopOrderEntity::getFulfillmentStatus, queryForm.getFulfillmentStatus());
        }
        if (queryForm.getRefundStatus() != null) {
            queryWrapper.eq(ShopOrderEntity::getRefundStatus, queryForm.getRefundStatus());
        }
        if (SmartStringUtil.isNotEmpty(queryForm.getSearchWord())) {
            queryWrapper.and(wrapper -> wrapper.like(ShopOrderEntity::getCustomerName, queryForm.getSearchWord())
                    .or()
                    .like(ShopOrderEntity::getCustomerEmail, queryForm.getSearchWord())
                    .or()
                    .like(ShopOrderEntity::getCustomerPhone, queryForm.getSearchWord())
                    .or()
                    .like(ShopOrderEntity::getOrderNo, queryForm.getSearchWord()));
        }
        queryWrapper.orderByDesc(ShopOrderEntity::getOrderId);

        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopOrderEntity> resultPage = shopOrderDao.selectPage((Page<ShopOrderEntity>) page, queryWrapper);
        List<ShopOrderVO> orderList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopOrderVO.class);
        fillItemCount(orderList);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, orderList));
    }

    /**
     * Get order detail.
     */
    public ResponseDTO<ShopOrderVO> detail(Long orderId) {
        ShopOrderEntity orderEntity = getValidOrder(orderId);
        if (orderEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopOrderVO orderVO = SmartBeanUtil.copy(orderEntity, ShopOrderVO.class);
        List<ShopOrderItemVO> itemList = queryOrderItemList(orderId);
        orderVO.setItemList(itemList);
        orderVO.setItemCount(itemList.size());
        return ResponseDTO.ok(orderVO);
    }

    /**
     * Update seller remark.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateRemark(ShopOrderRemarkForm remarkForm, RequestUser requestUser) {
        ShopOrderEntity orderEntity = getValidOrder(remarkForm.getOrderId());
        if (orderEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopOrderEntity updateEntity = new ShopOrderEntity();
        updateEntity.setOrderId(remarkForm.getOrderId());
        updateEntity.setSellerRemark(remarkForm.getSellerRemark());
        updateEntity.setUpdateUserId(requestUser.getUserId());
        shopOrderDao.updateById(updateEntity);
        return ResponseDTO.ok();
    }

    /**
     * Cancel unpaid order.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> cancel(ShopOrderCancelForm cancelForm, RequestUser requestUser) {
        ShopOrderEntity orderEntity = getValidOrder(cancelForm.getOrderId());
        if (orderEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }
        if (Objects.equals(orderEntity.getOrderStatus(), ORDER_STATUS_CANCELED)) {
            return ResponseDTO.userErrorParam("订单已取消");
        }
        if (Objects.equals(orderEntity.getOrderStatus(), ORDER_STATUS_COMPLETED)) {
            return ResponseDTO.userErrorParam("已完成订单不能取消");
        }
        if (Objects.equals(orderEntity.getPayStatus(), PAY_STATUS_PAID)) {
            return ResponseDTO.userErrorParam("已支付订单暂不支持取消，请先接入退款流程");
        }

        ShopOrderEntity updateEntity = new ShopOrderEntity();
        updateEntity.setOrderId(cancelForm.getOrderId());
        updateEntity.setOrderStatus(ORDER_STATUS_CANCELED);
        updateEntity.setCancelReason(cancelForm.getCancelReason());
        updateEntity.setUpdateUserId(requestUser.getUserId());
        shopOrderDao.updateById(updateEntity);
        return ResponseDTO.ok();
    }

    private ShopOrderEntity getValidOrder(Long orderId) {
        if (orderId == null) {
            return null;
        }
        LambdaQueryWrapper<ShopOrderEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopOrderEntity::getOrderId, orderId);
        queryWrapper.eq(ShopOrderEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopOrderDao.selectOne(queryWrapper);
    }

    private List<ShopOrderItemVO> queryOrderItemList(Long orderId) {
        LambdaQueryWrapper<ShopOrderItemEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopOrderItemEntity::getOrderId, orderId);
        queryWrapper.eq(ShopOrderItemEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.orderByAsc(ShopOrderItemEntity::getOrderItemId);
        return SmartBeanUtil.copyList(shopOrderItemDao.selectList(queryWrapper), ShopOrderItemVO.class);
    }

    private void fillItemCount(List<ShopOrderVO> orderList) {
        if (CollectionUtils.isEmpty(orderList)) {
            return;
        }
        List<Long> orderIdList = orderList.stream().map(ShopOrderVO::getOrderId).collect(Collectors.toList());
        LambdaQueryWrapper<ShopOrderItemEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(ShopOrderItemEntity::getOrderId, orderIdList);
        queryWrapper.eq(ShopOrderItemEntity::getDeletedFlag, Boolean.FALSE);
        List<ShopOrderItemEntity> itemList = shopOrderItemDao.selectList(queryWrapper);
        Map<Long, Long> countMap = CollectionUtils.isEmpty(itemList)
                ? Collections.emptyMap()
                : itemList.stream().collect(Collectors.groupingBy(ShopOrderItemEntity::getOrderId, Collectors.counting()));
        orderList.forEach(order -> order.setItemCount(countMap.getOrDefault(order.getOrderId(), 0L).intValue()));
    }
}
