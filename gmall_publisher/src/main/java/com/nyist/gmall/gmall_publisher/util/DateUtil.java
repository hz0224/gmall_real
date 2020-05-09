package com.nyist.gmall.gmall_publisher.util;

import org.apache.commons.lang.time.DateUtils;
import sun.applet.Main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Liu HangZhou on 2020/03/30
 * desc: 日期工具类
 */
public class DateUtil {

    private static SimpleDateFormat dateSimpledate = new SimpleDateFormat("yyyy-MM-dd");


    //获取指定日期的前一天
    public static String getYesterday(String dateString) {

        Date date = null;
        try {
            date = dateSimpledate.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterDay = DateUtils.addDays(date, -1);
        String yesterday = dateSimpledate.format(yesterDay);
        return yesterday;
    }


    public static void main(String[] args){

        System.out.println(getYesterday("2020-03-30"));
    }




}
