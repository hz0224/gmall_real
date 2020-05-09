package com.nyist.gmall.gmall_publisher.mapper;

import com.nyist.gmall.gmall_publisher.bean.DauHour;

import java.util.List;

/**
 * Created by Liu HangZhou on 2020/03/29
 * desc: 统计日活
 */
public interface DauMapper {

    //日活
    public long getDauTotal(String date);

    //分时活跃用户
    public List<DauHour> getDauHourTotal(String date);

}
