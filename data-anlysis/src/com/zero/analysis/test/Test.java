package com.zero.analysis.test;


import com.zero.analysis.client.AnalyticsEngineSDK;

/**
 * @author Alexander Zero
 * @version 1.0.0
 * @date 2021/12/21
 */
public class Test {
    public static void main(String[] args) {
        AnalyticsEngineSDK.onChargeSuccess("123424", "44635q35");
        AnalyticsEngineSDK.onChargeRefund("23154214", "324234");
    }
}
