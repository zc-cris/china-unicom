package com.cris.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 抓换时间的工具类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"JavaDoc", "WeakerAccess"})
public class DatetimeUtil {

    /**
     * 将时间字符串按照指定格式转换成时间戳，然后得到新的时间戳，再将新的时间戳转换为 Date，最后解析为时间字符串
     *
     * @param startDatetime 开始时间字符串
     * @param endDatetime   结束时间字符串
     * @param parse_format  解析格式
     * @return 格式化后的时间字符串
     * @throws ParseException
     */
    public static String parse(String startDatetime, String endDatetime, String parse_format) throws ParseException {

        DateFormat dateFormat = new SimpleDateFormat(parse_format);
        long startTime = dateFormat.parse(startDatetime).getTime();
        long endTime = dateFormat.parse(endDatetime).getTime();

        /*这里需要获取到一个时间差，使得到的通话时间确保在 startSecond 和 endSecond 之间且不包含它们*/
        long newTIme = startTime + (long) ((endTime - startTime - 1) * Math.random() + 1);

        Date date = new Date(newTIme);
        return dateFormat.format(date);
    }

    /**
     * 将时间字符串按照指定的格式转换为 LocalDateTime 类型（自动根据系统和时区封装的类型）
     *
     * @param datetime
     * @param parseFormat
     * @return
     * @throws ParseException
     */
    public static LocalDateTime parseStringToLocalDateTime(String datetime, String parseFormat) throws ParseException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(parseFormat);
        return LocalDateTime.parse(datetime, dateTimeFormatter);
    }

    /**
     * 将 LocalDateTime 类型的时间对象根据指定格式转换为字符串
     *
     * @param localDateTime
     * @param parseFormat
     * @return
     */
    public static String formatLocalDateTimeToString(LocalDateTime localDateTime, String parseFormat) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(parseFormat);
        return dateTimeFormatter.format(localDateTime);
    }

    public static void main(String[] args) throws ParseException {
        LocalDateTime localDateTime = parseStringToLocalDateTime("20181212181011", "yyyyMMddHHmmss");
        System.out.println(localDateTime);
        System.out.println("-------------------------------");
        String format = formatLocalDateTimeToString(localDateTime, "yyyyMMddHHmmss");
        System.out.println("format = " + format);

    }

}
