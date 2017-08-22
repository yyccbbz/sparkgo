package com.bigdata.test.java;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-21
 * Time: 18:31
 * Description:
 */
public class FieldTest {

    public static void main(String[] args) {

        /**
         * 数据格式：
         * 日期 用户 搜索词 城市 平台 版本
         */
        String[] dates = {"2017-08-20", "2017-08-21", "2017-08-22", "2017-08-23", "2017-08-24"};
        String[] users = {"leo", "tom", "jack", "marry", "white"};
        String[] words = {"barbecue", "seafood", "toy", "water", "tour"};
        String[] citys = {"beijing", "shanghai", "guangzhou", "shenzhen", "hangzhou"};
        String[] plats = {"Android", "IOS", "Windows", "MacOS", "Linux"};
        String[] verss = {"1.0", "2.0", "3.0", "4.0", "5.0"};

        for (int i = 0; i < 1000; i++) {
            int r1 = random5();
            int r2 = random5();
            int r3 = random5();
            int r4 = random5();
            int r5 = random5();
            int r6 = random5();
            System.out.println(
                    dates[r1] + "\t"
                            + users[r2] + "\t"
                            + words[r3] + "\t"
                            + citys[r4] + "\t"
                            + plats[r5] + "\t"
                            + verss[r6]
            );
        }

    }

    private static int random5() {
        Random random = new Random();
        return random.nextInt(5);
    }
}
