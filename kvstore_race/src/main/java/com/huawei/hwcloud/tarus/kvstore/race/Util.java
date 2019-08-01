package com.huawei.hwcloud.tarus.kvstore.race;

import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

/**
 * @author chenyuxi
 * @since 19-7-28:上午1:03
 */
public class Util {
    // 更快的转换
    public static long bytes2long(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }
    public static byte[] long2bytes(long key) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(key & 0xFF);
            key >>= 8;
        }
        return result;
    }
    public static String byte2String(byte[] bytes){
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            stringBuffer.append(bytes[i]);
        }
        return stringBuffer.toString();
    }
    public static byte[] genvalue(long key) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);
        buffer.putLong(key);
        for (int i = 0; i < 4048 - 8; ++i) {
            buffer.put((byte) 0);
        }
        return buffer.array();
    }
    public static void closeLinuxProcess(String Pid){
        Process process = null;
        BufferedReader reader =null;
        try{
            //杀掉进程
            process = Runtime.getRuntime().exec("kill -9 "+Pid);
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            while((line = reader.readLine())!=null){
                System.out.println("kill PID return info -----> "+line);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(process!=null){
                process.destroy();
            }

            if(reader!=null){
                try {
                    reader.close();
                } catch (IOException e) {

                }
            }
        }
    }
}
