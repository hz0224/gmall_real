package com.nyist.gmall.gmall_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @RequestMapping(value = "/log",method = RequestMethod.POST)
    public String getLog(@RequestParam("logString") String logString){

        //1 加上服务器时间
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        //2 写日志，用于离线分析
        logger.info(jsonObject.toString());

        //3 推送kafka，用于实时分析
        System.out.println(jsonObject.toString());

        return null;
    }

}
