package cn.just.shinelon;

import org.apache.commons.lang.StringUtils;

import java.util.Scanner;
public class OnSale {

    public static void main(String[] args) {
        convert();

    }

    public static void convert(){
        int b=20;
//        StringBuffer a=new StringBuffer();
        String a="";
        while(b>0){
            a+=String.valueOf(b%2);
//            a.append(String.valueOf(b%2));
            b=b/2;


        }
        System.out.println(StringUtils.reverse(a));
    }

}

