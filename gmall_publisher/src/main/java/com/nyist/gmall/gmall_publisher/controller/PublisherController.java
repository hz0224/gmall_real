package com.nyist.gmall.gmall_publisher.controller;

import com.alibaba.fastjson.JSON;
import com.nyist.gmall.gmall_publisher.bean.Option;
import com.nyist.gmall.gmall_publisher.bean.Stat;
import com.nyist.gmall.gmall_publisher.service.PublisherService;
import com.nyist.gmall.gmall_publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

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

    @RequestMapping(value = "/sale_detail",method = RequestMethod.GET)
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("keyword") String keyword,
                                @RequestParam("startpage") int startpage,@RequestParam("size") int size){

        //根据参数查询es
        Map<String, Object> saleDetailMap = publisherService.getSaleDetailFromES(date, keyword, startpage, size);


        Set<Map.Entry<String, Object>> entrySet = saleDetailMap.entrySet();
        for (Map.Entry<String, Object> entry : entrySet) {
            System.out.println(entry.getKey() + "----------" + entry.getValue());
        }


        Long total =(Long)saleDetailMap.get("total");
        List saleList =(List) saleDetailMap.get("saleList");
        Map genderMap =(Map) saleDetailMap.get("genderMap");
        Map ageMap = (Map)saleDetailMap.get("ageMap");


        Long maleCount =(Long)genderMap.get("M");
        Long femaleCount =(Long)genderMap.get("F");


        Double maleRatio= Math.round(maleCount*1000D/total)/10D;
        Double femaleRatio= Math.round(femaleCount*1000D/total)/10D;

        List genderOptionList=new ArrayList();
        genderOptionList.add( new Option("男",maleRatio));
        genderOptionList.add( new Option("女",femaleRatio));

        Stat genderStat = new Stat("性别占比", genderOptionList);


        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageString = (String)entry.getKey();
            Long ageCount = (Long)entry.getValue();
            if(Integer.parseInt(ageString)<20){
                age_20count+=ageCount;
            }else if(Integer.parseInt(ageString)>=20 &&Integer.parseInt(ageString)<=30){
                age20_30count+=ageCount;
            }else{
                age30_count+=ageCount;
            }
        }

        Double age_20Ratio= Math.round(age_20count*1000D/total)/10D;
        Double age20_30Ratio= Math.round(age20_30count*1000D/total)/10D;
        Double age30_Ratio= Math.round(age30_count*1000D/total)/10D;

        List ageOptionList=new ArrayList();
        ageOptionList.add( new Option("20岁以下",age_20Ratio));
        ageOptionList.add( new Option("20岁到30岁",age20_30Ratio));
        ageOptionList.add( new Option("30岁以上",age30_Ratio));

        Stat ageStat = new Stat("年龄段占比", ageOptionList);

        List statList=new ArrayList();
        statList.add(genderStat);
        statList.add(ageStat);

        System.out.println(statList);
        System.out.println(saleList);

        Map finalResultMap=new HashMap();
        finalResultMap.put("total",total);
        finalResultMap.put("stat",statList);
        finalResultMap.put("detail" ,saleList);


        return JSON.toJSONString(finalResultMap);
    }

}
