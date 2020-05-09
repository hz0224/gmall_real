package com.nyist.gmall.gmall_publisher.service.impl;

import com.nyist.gmall.gmall_publisher.bean.DauHour;
import com.nyist.gmall.gmall_publisher.bean.OrderAmountHour;
import com.nyist.gmall.gmall_publisher.mapper.DauMapper;
import com.nyist.gmall.gmall_publisher.mapper.OrderMapper;
import com.nyist.gmall.gmall_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourTotal(String date) {

        List<DauHour> dauHourTotal = dauMapper.getDauHourTotal(date);
        HashMap<String, Long> map = new HashMap<>();

        for (DauHour dauHour : dauHourTotal) {
            map.put(dauHour.getLogHour(),dauHour.getCt());
        }
        return map;
    }

    @Override
    public double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map<String, Object> getOrderAmountHourTotal(String date) {

        List<OrderAmountHour> orderAmountHourTotal = orderMapper.getOrderAmountHourTotal(date);
        HashMap<String, Object> map = new HashMap<>();

        for (OrderAmountHour orderAmountHour : orderAmountHourTotal) {
            map.put(orderAmountHour.getCreateHour(),orderAmountHour.getTotalAmount());
        }
        return map;
    }


}
