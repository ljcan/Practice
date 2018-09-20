package cn.just.shinelon.utils;


import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

/**
 * 使用单例模式构建
 */
public class DateUtil {

    private DateUtil(){
    }

    private static DateUtil instance;

    public static DateUtil getInstance(){
        if(instance==null){
            return new DateUtil();
        }
        return instance;
    }

    public FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public long getTime(String time) throws Exception {
        return dateFormat.parse(time).getTime();
    }


}
