package com.nyist.gmall.gmall_publisher.service;

import java.util.Map;

public interface PublisherService {

    public long getDauTotal(String date);

    public Map<String,Long> getDauHourTotal(String date);

    public double getOrderAmountTotal(String date);

    public Map<String,Object> getOrderAmountHourTotal(String date);

    public Map<String,Object> getSaleDetailFromES(String date,String keyword,int pageNo,int pagesize);


}
