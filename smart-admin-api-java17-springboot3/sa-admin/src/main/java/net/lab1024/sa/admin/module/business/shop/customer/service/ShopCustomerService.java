package net.lab1024.sa.admin.module.business.shop.customer.service;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.annotation.Resource;
import net.lab1024.sa.admin.module.business.shop.customer.dao.ShopCustomerAddressDao;
import net.lab1024.sa.admin.module.business.shop.customer.dao.ShopCustomerDao;
import net.lab1024.sa.admin.module.business.shop.customer.domain.entity.ShopCustomerAddressEntity;
import net.lab1024.sa.admin.module.business.shop.customer.domain.entity.ShopCustomerEntity;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerLoginForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerQueryForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerRegisterForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerRemarkForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.form.ShopCustomerUpdateDisabledForm;
import net.lab1024.sa.admin.module.business.shop.customer.domain.vo.ShopCustomerAddressVO;
import net.lab1024.sa.admin.module.business.shop.customer.domain.vo.ShopCustomerLoginVO;
import net.lab1024.sa.admin.module.business.shop.customer.domain.vo.ShopCustomerVO;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.PageResult;
import net.lab1024.sa.base.common.domain.RequestUser;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.common.util.SmartBeanUtil;
import net.lab1024.sa.base.common.util.SmartPageUtil;
import net.lab1024.sa.base.common.util.SmartStringUtil;
import net.lab1024.sa.base.module.support.securityprotect.service.SecurityPasswordService;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shop customer service.
 */
@Service
public class ShopCustomerService {

    private static final Long DEFAULT_TENANT_ID = 1L;

    private static final Integer REGISTER_SOURCE_EMAIL = 1;

    private static final Integer REGISTER_SOURCE_PHONE = 2;

    @Resource
    private ShopCustomerDao shopCustomerDao;

    @Resource
    private ShopCustomerAddressDao shopCustomerAddressDao;

    /**
     * Query customer page.
     */
    public ResponseDTO<PageResult<ShopCustomerVO>> queryPage(ShopCustomerQueryForm queryForm) {
        LambdaQueryWrapper<ShopCustomerEntity> queryWrapper = new LambdaQueryWrapper<>();
        if (queryForm.getTenantId() != null) {
            queryWrapper.eq(ShopCustomerEntity::getTenantId, queryForm.getTenantId());
        }
        queryWrapper.eq(ShopCustomerEntity::getDeletedFlag, Boolean.FALSE);
        if (queryForm.getDisabledFlag() != null) {
            queryWrapper.eq(ShopCustomerEntity::getDisabledFlag, queryForm.getDisabledFlag());
        }
        if (SmartStringUtil.isNotEmpty(queryForm.getSearchWord())) {
            queryWrapper.and(wrapper -> wrapper.like(ShopCustomerEntity::getCustomerNo, queryForm.getSearchWord())
                    .or()
                    .like(ShopCustomerEntity::getCustomerName, queryForm.getSearchWord())
                    .or()
                    .like(ShopCustomerEntity::getEmail, queryForm.getSearchWord())
                    .or()
                    .like(ShopCustomerEntity::getPhone, queryForm.getSearchWord()));
        }
        queryWrapper.orderByDesc(ShopCustomerEntity::getCustomerId);

        Page<?> page = SmartPageUtil.convert2PageQuery(queryForm);
        Page<ShopCustomerEntity> resultPage = shopCustomerDao.selectPage((Page<ShopCustomerEntity>) page, queryWrapper);
        List<ShopCustomerVO> customerList = SmartBeanUtil.copyList(resultPage.getRecords(), ShopCustomerVO.class);
        fillAddressCount(customerList);
        return ResponseDTO.ok(SmartPageUtil.convert2PageResult(resultPage, customerList));
    }

    /**
     * Get customer detail.
     */
    public ResponseDTO<ShopCustomerVO> detail(Long customerId) {
        ShopCustomerEntity customerEntity = getValidCustomer(customerId);
        if (customerEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopCustomerVO customerVO = SmartBeanUtil.copy(customerEntity, ShopCustomerVO.class);
        List<ShopCustomerAddressVO> addressList = queryAddressList(customerId);
        customerVO.setAddressList(addressList);
        customerVO.setAddressCount(addressList.size());
        return ResponseDTO.ok(customerVO);
    }

    /**
     * Update customer disabled flag.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateDisabled(ShopCustomerUpdateDisabledForm updateForm, RequestUser requestUser) {
        ShopCustomerEntity customerEntity = getValidCustomer(updateForm.getCustomerId());
        if (customerEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopCustomerEntity updateEntity = new ShopCustomerEntity();
        updateEntity.setCustomerId(updateForm.getCustomerId());
        updateEntity.setDisabledFlag(updateForm.getDisabledFlag());
        updateEntity.setUpdateUserId(requestUser.getUserId());
        shopCustomerDao.updateById(updateEntity);
        return ResponseDTO.ok();
    }

    /**
     * Update customer remark.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<String> updateRemark(ShopCustomerRemarkForm remarkForm, RequestUser requestUser) {
        ShopCustomerEntity customerEntity = getValidCustomer(remarkForm.getCustomerId());
        if (customerEntity == null) {
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST);
        }

        ShopCustomerEntity updateEntity = new ShopCustomerEntity();
        updateEntity.setCustomerId(remarkForm.getCustomerId());
        updateEntity.setRemark(remarkForm.getRemark());
        updateEntity.setUpdateUserId(requestUser.getUserId());
        shopCustomerDao.updateById(updateEntity);
        return ResponseDTO.ok();
    }

    /**
     * Register C-side customer.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<ShopCustomerLoginVO> register(ShopCustomerRegisterForm registerForm) {
        Long tenantId = getTenantId(registerForm.getTenantId());
        String email = trimToNull(registerForm.getEmail());
        String phone = trimToNull(registerForm.getPhone());
        if (SmartStringUtil.isEmpty(email) && SmartStringUtil.isEmpty(phone)) {
            return ResponseDTO.userErrorParam("邮箱和手机号至少填写一个");
        }
        ResponseDTO<String> uniqueResult = validateAccountUnique(tenantId, email, phone);
        if (!uniqueResult.getOk()) {
            return ResponseDTO.error(uniqueResult);
        }

        String token = IdUtil.fastSimpleUUID();
        LocalDateTime now = LocalDateTime.now();
        ShopCustomerEntity customerEntity = new ShopCustomerEntity();
        customerEntity.setTenantId(tenantId);
        customerEntity.setCustomerNo(generateCustomerNo(tenantId));
        customerEntity.setCustomerName(buildCustomerName(registerForm.getCustomerName(), email, phone));
        customerEntity.setEmail(email);
        customerEntity.setPhone(phone);
        customerEntity.setLoginPwd(SecurityPasswordService.getEncryptPwd(registerForm.getPassword()));
        customerEntity.setRegisterSource(SmartStringUtil.isNotEmpty(email) ? REGISTER_SOURCE_EMAIL : REGISTER_SOURCE_PHONE);
        customerEntity.setDisabledFlag(Boolean.FALSE);
        customerEntity.setDeletedFlag(Boolean.FALSE);
        customerEntity.setLastLoginTime(now);
        customerEntity.setLoginToken(token);
        shopCustomerDao.insert(customerEntity);

        return ResponseDTO.ok(buildLoginVO(customerEntity, token));
    }

    /**
     * Login C-side customer.
     */
    @Transactional(rollbackFor = Exception.class)
    public ResponseDTO<ShopCustomerLoginVO> login(ShopCustomerLoginForm loginForm) {
        Long tenantId = getTenantId(loginForm.getTenantId());
        ShopCustomerEntity customerEntity = getCustomerByAccount(tenantId, loginForm.getAccount());
        if (customerEntity == null || !SecurityPasswordService.matchesPwd(loginForm.getPassword(), customerEntity.getLoginPwd())) {
            return ResponseDTO.userErrorParam("账号或密码错误");
        }
        if (Objects.equals(customerEntity.getDisabledFlag(), Boolean.TRUE)) {
            return ResponseDTO.userErrorParam("客户已禁用");
        }

        String token = IdUtil.fastSimpleUUID();
        ShopCustomerEntity updateEntity = new ShopCustomerEntity();
        updateEntity.setCustomerId(customerEntity.getCustomerId());
        updateEntity.setLoginToken(token);
        updateEntity.setLastLoginTime(LocalDateTime.now());
        shopCustomerDao.updateById(updateEntity);

        return ResponseDTO.ok(buildLoginVO(customerEntity, token));
    }

    private ShopCustomerEntity getValidCustomer(Long customerId) {
        if (customerId == null) {
            return null;
        }
        LambdaQueryWrapper<ShopCustomerEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCustomerEntity::getCustomerId, customerId);
        queryWrapper.eq(ShopCustomerEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.last("limit 1");
        return shopCustomerDao.selectOne(queryWrapper);
    }

    private ShopCustomerEntity getCustomerByAccount(Long tenantId, String account) {
        LambdaQueryWrapper<ShopCustomerEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCustomerEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCustomerEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.and(wrapper -> wrapper.eq(ShopCustomerEntity::getEmail, account)
                .or()
                .eq(ShopCustomerEntity::getPhone, account));
        queryWrapper.last("limit 1");
        return shopCustomerDao.selectOne(queryWrapper);
    }

    private List<ShopCustomerAddressVO> queryAddressList(Long customerId) {
        LambdaQueryWrapper<ShopCustomerAddressEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCustomerAddressEntity::getCustomerId, customerId);
        queryWrapper.eq(ShopCustomerAddressEntity::getDeletedFlag, Boolean.FALSE);
        queryWrapper.orderByDesc(ShopCustomerAddressEntity::getDefaultFlag);
        queryWrapper.orderByDesc(ShopCustomerAddressEntity::getAddressId);
        return SmartBeanUtil.copyList(shopCustomerAddressDao.selectList(queryWrapper), ShopCustomerAddressVO.class);
    }

    private void fillAddressCount(List<ShopCustomerVO> customerList) {
        if (CollectionUtils.isEmpty(customerList)) {
            return;
        }
        List<Long> customerIdList = customerList.stream().map(ShopCustomerVO::getCustomerId).collect(Collectors.toList());
        LambdaQueryWrapper<ShopCustomerAddressEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(ShopCustomerAddressEntity::getCustomerId, customerIdList);
        queryWrapper.eq(ShopCustomerAddressEntity::getDeletedFlag, Boolean.FALSE);
        List<ShopCustomerAddressEntity> addressList = shopCustomerAddressDao.selectList(queryWrapper);
        Map<Long, Long> countMap = CollectionUtils.isEmpty(addressList)
                ? Collections.emptyMap()
                : addressList.stream().collect(Collectors.groupingBy(ShopCustomerAddressEntity::getCustomerId, Collectors.counting()));
        customerList.forEach(customer -> customer.setAddressCount(countMap.getOrDefault(customer.getCustomerId(), 0L).intValue()));
    }

    private ResponseDTO<String> validateAccountUnique(Long tenantId, String email, String phone) {
        if (SmartStringUtil.isNotEmpty(email) && accountExists(tenantId, true, email)) {
            return ResponseDTO.userErrorParam("邮箱已注册");
        }
        if (SmartStringUtil.isNotEmpty(phone) && accountExists(tenantId, false, phone)) {
            return ResponseDTO.userErrorParam("手机号已注册");
        }
        return ResponseDTO.ok();
    }

    private boolean accountExists(Long tenantId, boolean emailFlag, String account) {
        LambdaQueryWrapper<ShopCustomerEntity> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ShopCustomerEntity::getTenantId, tenantId);
        queryWrapper.eq(ShopCustomerEntity::getDeletedFlag, Boolean.FALSE);
        if (emailFlag) {
            queryWrapper.eq(ShopCustomerEntity::getEmail, account);
        } else {
            queryWrapper.eq(ShopCustomerEntity::getPhone, account);
        }
        queryWrapper.last("limit 1");
        return shopCustomerDao.selectOne(queryWrapper) != null;
    }

    private String generateCustomerNo(Long tenantId) {
        for (int i = 0; i < 5; i++) {
            String customerNo = "C" + System.currentTimeMillis() + RandomUtil.randomNumbers(4);
            LambdaQueryWrapper<ShopCustomerEntity> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.eq(ShopCustomerEntity::getTenantId, tenantId);
            queryWrapper.eq(ShopCustomerEntity::getCustomerNo, customerNo);
            queryWrapper.last("limit 1");
            if (shopCustomerDao.selectOne(queryWrapper) == null) {
                return customerNo;
            }
        }
        return "C" + IdUtil.fastSimpleUUID().substring(0, 16).toUpperCase();
    }

    private ShopCustomerLoginVO buildLoginVO(ShopCustomerEntity customerEntity, String token) {
        ShopCustomerLoginVO loginVO = SmartBeanUtil.copy(customerEntity, ShopCustomerLoginVO.class);
        loginVO.setToken(token);
        return loginVO;
    }

    private String buildCustomerName(String customerName, String email, String phone) {
        String trimmedName = trimToNull(customerName);
        if (SmartStringUtil.isNotEmpty(trimmedName)) {
            return trimmedName;
        }
        return SmartStringUtil.isNotEmpty(email) ? email : phone;
    }

    private String trimToNull(String value) {
        if (SmartStringUtil.isEmpty(value)) {
            return null;
        }
        String trimValue = value.trim();
        return SmartStringUtil.isEmpty(trimValue) ? null : trimValue;
    }

    private Long getTenantId(Long tenantId) {
        return tenantId == null ? DEFAULT_TENANT_ID : tenantId;
    }
}
