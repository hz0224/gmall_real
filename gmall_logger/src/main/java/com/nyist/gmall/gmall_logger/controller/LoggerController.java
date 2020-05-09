package com.nyist.gmall.gmall_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Liu HangZhou on 2020/03/27
 * desc: 日志控制器
 */


@RestController
public class LoggerController {

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping(value = "/log",method = RequestMethod.POST)
    public String getLog(@RequestParam("logString") String logString){
        //1 加上服务器时间
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        String logJson = jsonObject.toString();
        //2 写日志，用于离线分析
        logger.info(logJson);
        //3 推送kafka，用于实时分析
        if ("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,logJson);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,logJson);
        }

        return "success";
    }

}
