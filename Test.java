package com.fofund.bus.domain.biz;

import com.baomidou.dynamic.datasource.annotation.DSTransactional;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.fofund.boot.redis.cache.RedisService;
import com.fofund.boot.rocketmq.service.ArkRocketMqService;
import com.fofund.bus.common.enums.DeleteFlagEnum;
import com.fofund.bus.common.enums.RelationChannelTypeEnum;
import com.fofund.bus.common.enums.ResponseEnum;
import com.fofund.bus.common.exception.CrmBusinessException;
import com.fofund.bus.common.util.BusCrmDateUtil;
import com.fofund.bus.common.util.CommonUtil;
import com.fofund.bus.common.util.CommonUtil.UpdateCollector;
import com.fofund.bus.domain.bo.crmUserFundRelation.OpsCrmTradeSalesRelationBO;
import com.fofund.bus.domain.bo.crmUserFundRelation.OpsCrmUserFundRelationBO;
import com.fofund.bus.domain.convert.Bo2DoConvert;
import com.fofund.bus.domain.convert.Do2BoConvert;
import com.fofund.bus.domain.mq.MQConstant;
import com.fofund.bus.domain.mq.message.MaintainerChangedMessage;
import com.fofund.bus.model.CrmUserFundRelationConfigDO;
import com.fofund.bus.model.EtsBrokerTradeDivideDO;
import com.fofund.bus.model.OpsCrmTradeSalesRelationDO;
import com.fofund.bus.repository.CrmUserFundRelationConfigDao;
import com.fofund.bus.repository.OpsCrmTradeSalesRelationDao;
import com.fofund.common.utils.DateUtil;
import com.fofund.ipmc.pojo.response.AccountResponse;
import com.google.common.collect.Lists;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static com.fofund.bus.common.constant.CrmRedisLockKey.BIND_PRODUCT_ACCOUNT;

/**
 * 交易绑定维护人相关
 *
 * @author 肖军
 * @version 1.0
 * @date 2023-05-04 10:53
 */
@Component
@Slf4j
public class OpsTradeContactPersonSaveLogic {


    @Resource
    private RedisService redisService;
    @Resource
    private OpsCrmTradeSalesRelationDao opsCrmTradeSalesRelationDao;
    @Resource
    private CrmUserFundRelationConfigLogic crmUserFundRelationConfigLogic;
    @Resource
    private EtsBrokerTradeDivideLogic etsBrokerTradeDivideLogic;
    @Resource
    private ArkRocketMqService arkRocketMqService;
    @Resource
    private CrmUserFundRelationConfigDao configDao;

    /**
     * 导入保存
     */
//    public void saveContactPersonImport(List<OpsCrmTradeSalesRelationDO> newList, List<CrmUserFundRelationConfigDO> userFund){
//        //查询主表
//        UpdateWrapper<OpsCrmTradeSalesRelationDO> tradeWrapper = new UpdateWrapper<>();
//        tradeWrapper.eq("delete_flag", DeleteFlagEnum.DELETE_FLAG_0.getCode());
//        tradeWrapper.in("trade_acco", newList.stream().map(OpsCrmTradeSalesRelationDO::getTradeAcco).distinct().collect(Collectors.toList()));
//        List<OpsCrmTradeSalesRelationDO> oldList = opsCrmTradeSalesRelationDao.list(tradeWrapper);
//
//        //解析数据
//        List<OpsCrmTradeSalesRelationBO> newListBO = Do2BoConvert.INSTANCE.convertCrmTradeSales(newList);
//        List<OpsCrmTradeSalesRelationBO> oldListBO = Do2BoConvert.INSTANCE.convertCrmTradeSales(oldList);
//        UpdateCollector<OpsCrmTradeSalesRelationBO> tradeSales = CommonUtil.updateDataCollect(newListBO, oldListBO);
//
//        //查询关联，存在不绑定产品的
//        List<CrmUserFundRelationConfigDO> userList = Lists.newArrayList();
//        if(!CollectionUtils.isEmpty(userFund)){
//            QueryWrapper<CrmUserFundRelationConfigDO> userWrapper = new QueryWrapper<>();
//            userWrapper.lambda().eq(CrmUserFundRelationConfigDO::getDeleteFlag,  DeleteFlagEnum.DELETE_FLAG_0.getCode())
//                    .eq(CrmUserFundRelationConfigDO::getChannelType,  RelationChannelTypeEnum.CHANNEL_TYPE_TRADE.getCode())
//                    .in(CrmUserFundRelationConfigDO::getRelationId, userFund.stream().map(CrmUserFundRelationConfigDO::getRelationId).distinct().collect(Collectors.toList()))
//                    .select(CrmUserFundRelationConfigDO::getId, CrmUserFundRelationConfigDO::getRelationId,
//                            CrmUserFundRelationConfigDO::getUserId,CrmUserFundRelationConfigDO::getUserType);
//            userList.addAll(configDao.list(userWrapper));
//        }
//        //解析数据
//        List<OpsCrmUserFundRelationBO> newUserListBO = Do2BoConvert.INSTANCE.convertUserFundRelation(userFund);
//        List<OpsCrmUserFundRelationBO> oldUserListBO = Do2BoConvert.INSTANCE.convertUserFundRelation(userList);
//        long b = System.currentTimeMillis();
//        UpdateCollector<OpsCrmUserFundRelationBO> userSales = CommonUtil.updateDataCollect(newUserListBO, oldUserListBO);
//        System.out.printf("OpsCrmUserFundRelationBO处理：%d 毫秒.", (System.currentTimeMillis() - b));
//
//        //写入关联关系
//        long mainTime = System.currentTimeMillis();
//        crmUserFundRelationConfigLogic.saveCrmUserFundRelation(tradeSales,userSales);
//        System.out.printf("主表逻辑处理：%d 毫秒.", (System.currentTimeMillis() - mainTime));
//    }

    /**
     * 导入保存
     */
    @DSTransactional
    public void saveContactPersonImport(List<OpsCrmTradeSalesRelationDO> newList){
        //查询主表
        UpdateWrapper<OpsCrmTradeSalesRelationDO> tradeWrapper = new UpdateWrapper<>();
        tradeWrapper.lambda().eq(OpsCrmTradeSalesRelationDO::getDeleteFlag, DeleteFlagEnum.DELETE_FLAG_0.getCode());
        tradeWrapper.lambda().in(OpsCrmTradeSalesRelationDO::getTradeAcco, newList.stream().map(OpsCrmTradeSalesRelationDO::getTradeAcco).distinct().collect(Collectors.toList()));
        List<OpsCrmTradeSalesRelationDO> oldList = opsCrmTradeSalesRelationDao.list(tradeWrapper);

        //解析数据
        List<OpsCrmTradeSalesRelationBO> newListBO = Do2BoConvert.INSTANCE.convertCrmTradeSales(newList);
        List<OpsCrmTradeSalesRelationBO> oldListBO = Do2BoConvert.INSTANCE.convertCrmTradeSales(oldList);
        UpdateCollector<OpsCrmTradeSalesRelationBO> tradeSales = CommonUtil.updateDataCollect(newListBO, oldListBO);

        //写入关联关系
        long mainTime = System.currentTimeMillis();
        //新增
        List<OpsCrmTradeSalesRelationBO> tradeAdd = tradeSales.getNeedInsertList();
        if(!CollectionUtils.isEmpty(tradeAdd)){
            List<OpsCrmTradeSalesRelationDO> save = Bo2DoConvert.INSTANCE.convertTradeSales(tradeAdd);
            opsCrmTradeSalesRelationDao.saveBatch(save);
        }
        //删除
        List<OpsCrmTradeSalesRelationBO> tradeDel = tradeSales.getNeedDeleteList();
        if(!CollectionUtils.isEmpty(tradeDel)){
            tradeDel.forEach(d ->{
                d.setDeleteFlag(DeleteFlagEnum.DELETE_FLAG_1.getCode());
                d.setUpdateTime(new Date());
            });
            List<OpsCrmTradeSalesRelationDO> update = Bo2DoConvert.INSTANCE.convertTradeSales(tradeDel);
            opsCrmTradeSalesRelationDao.updateBatchById(update);
        }
        //crmUserFundRelationConfigLogic.saveCrmUserFundRelation(tradeSales,userSales);
       log.info("主表逻辑处理：%d 毫秒.{}", (System.currentTimeMillis() - mainTime));
    }

    @Async
    @DSTransactional
    public void asyncSaveUserFundRelation(List<CrmUserFundRelationConfigDO> userFund){
        try {
            boolean lock = redisService.setIfAbsent(BIND_PRODUCT_ACCOUNT, System.currentTimeMillis(), 30 * 60L);
            if (!lock) {
                log.warn("[#calculate#]交易账号绑定维护人导入，拆分至产品与维护人关系处理中，请稍后处理");
            }
            //查询关联，存在不绑定产品的
            List<CrmUserFundRelationConfigDO> userList = Lists.newArrayList();
            if (!CollectionUtils.isEmpty(userFund)) {
                QueryWrapper<CrmUserFundRelationConfigDO> userWrapper = new QueryWrapper<>();
                userWrapper.lambda().eq(CrmUserFundRelationConfigDO::getDeleteFlag, DeleteFlagEnum.DELETE_FLAG_0.getCode())
                        .eq(CrmUserFundRelationConfigDO::getChannelType, RelationChannelTypeEnum.CHANNEL_TYPE_TRADE.getCode())
                        .in(CrmUserFundRelationConfigDO::getRelationId, userFund.stream().map(CrmUserFundRelationConfigDO::getRelationId).distinct().collect(Collectors.toList()))
                        .select(CrmUserFundRelationConfigDO::getId, CrmUserFundRelationConfigDO::getRelationId,
                                CrmUserFundRelationConfigDO::getUserId, CrmUserFundRelationConfigDO::getUserType);
                userList.addAll(configDao.list(userWrapper));
            }
            //解析数据
            List<OpsCrmUserFundRelationBO> newUserListBO = Do2BoConvert.INSTANCE.convertUserFundRelation(userFund);
            List<OpsCrmUserFundRelationBO> oldUserListBO = Do2BoConvert.INSTANCE.convertUserFundRelation(userList);
            long b = System.currentTimeMillis();
            UpdateCollector<OpsCrmUserFundRelationBO> userSales = CommonUtil.updateDataCollect(newUserListBO, oldUserListBO);
            log.info("OpsCrmUserFundRelationBO处理：%d 毫秒:{}", (System.currentTimeMillis() - b));

            //写入关联关系
            long mainTime = System.currentTimeMillis();
            //新增
            List<OpsCrmUserFundRelationBO> userAdd = userSales.getNeedInsertList();
            if (!CollectionUtils.isEmpty(userAdd)) {
                List<CrmUserFundRelationConfigDO> save = Bo2DoConvert.INSTANCE.converUserFundRelation(userAdd);
                configDao.saveBatch(save);
            }
            //删除
            List<OpsCrmUserFundRelationBO> userDel = userSales.getNeedDeleteList();
            if (!CollectionUtils.isEmpty(userDel)) {
                userDel.forEach(d -> {
                    d.setDeleteFlag(DeleteFlagEnum.DELETE_FLAG_1.getCode());
                    d.setLastModifyTime(BusCrmDateUtil.getTime());
                });
                List<CrmUserFundRelationConfigDO> update = Bo2DoConvert.INSTANCE.converUserFundRelation(userDel);
                configDao.updateBatchById(update);
            }
            log.info("主表逻辑处理：%d 毫秒{}", (System.currentTimeMillis() - mainTime));
        }catch (Exception e){
            log.error("交易账号{}绑定维护人导入，拆分至产品与维护人关系处理中，处理异常",userFund.stream().map(CrmUserFundRelationConfigDO::getRelationId).collect(Collectors.toSet()));
            throw new CrmBusinessException(ResponseEnum.BIND_PRD_ACCO_EXEC);
        }finally {
            redisService.del(BIND_PRODUCT_ACCOUNT);
        }

    }

    @DSTransactional
    public void removeContactPersonImport(List<String> tradeAcco){
        //主
        UpdateWrapper<OpsCrmTradeSalesRelationDO> wrapper = new UpdateWrapper<>();
        wrapper.eq("delete_flag", DeleteFlagEnum.DELETE_FLAG_0.getCode());
        wrapper.in("trade_acco",tradeAcco);
        opsCrmTradeSalesRelationDao.remove(wrapper);

        //关系
        crmUserFundRelationConfigLogic.removeCrmUserFundRelation(tradeAcco);

        //ets
        etsBrokerTradeDivideLogic.removeBrokerTradeDivide(tradeAcco);
    }

    /**
     * 回写etsprd异步
     */
    @DSTransactional
    public void saveContactPersonImportAsync(Map<String,AccountResponse> accountMap,List<EtsBrokerTradeDivideDO> saveBroker,List<String> accountNameList){
        etsBrokerTradeDivideLogic.saveBrokerTradeDivide(saveBroker,accountMap,accountNameList);
    }

    /**
     * 回写etsprd
     */
    @DSTransactional
    public void saveTradeContactPersonAsync(List<OpsCrmTradeSalesRelationDO> list, Map<String,AccountResponse> nameMap,Map<String,AccountResponse> codeMap,
            String userCode,List<String> accountNameList){
        List<EtsBrokerTradeDivideDO> saveBroker =Lists.newArrayList();
        list.forEach(x->{
            EtsBrokerTradeDivideDO save = new EtsBrokerTradeDivideDO();
            AccountResponse code = codeMap.get(x.getAccountCode());
            if(null !=code){
                save.setAccountCode(code.getLoginName());
                save.setEmployeeName(code.getEmployeeName());
            }
            save.setId(x.getId());
            save.setCreateTime(new Date());
            save.setStartDate(DateUtil.getDate());
            save.setTradeAcco(x.getTradeAcco());
            save.setDivideType(x.getDivideType());
            save.setCreateUser(userCode);
            saveBroker.add(save);
        });
        etsBrokerTradeDivideLogic.saveBrokerTradeDivide(saveBroker,nameMap,accountNameList);
    }

    /**
     * 异步发送mq通知
     */
    public void asyncSendMq(List<String> tradeAcco) {
        MaintainerChangedMessage message = new MaintainerChangedMessage();
        message.setTradeAccoList(tradeAcco);
        message.setChannel(RelationChannelTypeEnum.CHANNEL_TYPE_TRADE.getCode());
        log.info("修改交易账号绑定联系人,Topic:{} Message: {}", MQConstant.BUS_CRM_SYNC_PRODUCT_MANAGE_TOPIC, message);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
                arkRocketMqService.asyncSend(MQConstant.BUS_CRM_SYNC_PRODUCT_MANAGE_TOPIC, message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        log.info("修改交易账号绑定联系人，通知产品中心mq发送成功，发送结果:{}", sendResult);
                    }
                    @Override
                    public void onException(Throwable throwable) {
                        log.error("修改交易账号绑定联系人，通知产品中心mq发送出现异常", throwable);
                    }
                });
            }
        });
    }


}
