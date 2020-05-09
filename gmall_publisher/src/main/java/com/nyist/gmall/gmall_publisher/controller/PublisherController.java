package com.nyist.gmall.gmall_publisher.controller;

import com.alibaba.fastjson.JSON;
import com.nyist.gmall.gmall_publisher.service.PublisherService;
import com.nyist.gmall.gmall_publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Liu HangZhou on 2020/03/30
 * desc: 发布web接口
 */

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    //这个请求用来处理求总和的需求.       如日活，新增设备.
    @RequestMapping(value = "/realtime-total",method = RequestMethod.GET)
    public String getTotal(@RequestParam("date") String date){

        //处理日活
        long dauTotal = publisherService.getDauTotal(date);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        list.add(dauMap);

        //处理交易额
        HashMap<String, Object> orderAmountMap = new HashMap<>();
        double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmountTotal);
        list.add(orderAmountMap);

        return JSON.toJSONString(list);
    }

    //分时数据，有昨天和今天的,做对比.
    @RequestMapping(value = "/realtime-hour",method = RequestMethod.GET)
    public String getTotalHour(@RequestParam("id") String id,@RequestParam("date") String date){

        switch (id){
            case "dau" : return handleDauHour(date);
            case "order_amount" : return handleOrderAmountHour(date);
            default:
        }

        return null;
    }

    //处理分时日活
    public String handleDauHour(String date){
        HashMap<String, Object> map = new HashMap<>();
        //查询昨天(相对于今天)
        String yesterday = DateUtil.getYesterday(date);
        Map<String, Long> yesterdayMap = publisherService.getDauHourTotal(yesterday);
        map.put("yesterday",yesterdayMap);
        //查询今天
        Map<String, Long> todayMap = publisherService.getDauHourTotal(date);
        map.put("today",todayMap);
        return JSON.toJSONString(map);
    }

    //处理分时交易额
    public String handleOrderAmountHour(String date){
        HashMap<String, Object> map = new HashMap<String, Object>();
        String yesterday = DateUtil.getYesterday(date);
        Map<String, Object> yesterdayOrderAmountHourTotal = publisherService.getOrderAmountHourTotal(yesterday);
        map.put("yesterday",yesterdayOrderAmountHourTotal);
        Map<String, Object> todayOrderAmountHourTotal = publisherService.getOrderAmountHourTotal(date);
        map.put("today",todayOrderAmountHourTotal);
        return JSON.toJSONString(map);
    }
}
