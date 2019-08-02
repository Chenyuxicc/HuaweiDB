package com.huawei.hwcloud.tarus.kvstore.test;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class Util {

    public static long bytes2Long(byte[] buffer) {
        long values = 0;
        int len = 8;
        for (int i = 0; i < len; ++i) {
            values <<= 8;
            values |= (buffer[i] & 0xff);
        }
        return values;
    }

    public static byte[] long2bytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; ++i) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    public static long parseKey(String key) {
        return Long.valueOf(key);
    }

}
