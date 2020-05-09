package com.nyist.gmall.gmall_publisher.mapper;

import com.nyist.gmall.gmall_publisher.bean.OrderAmountHour;

import java.util.List;

public interface OrderMapper {

    public double getOrderAmountTotal(String date);

    public List<OrderAmountHour> getOrderAmountHourTotal(String date);

}
