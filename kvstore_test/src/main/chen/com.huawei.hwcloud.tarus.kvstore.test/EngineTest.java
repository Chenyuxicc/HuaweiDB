package com.huawei.hwcloud.tarus.kvstore.test;

import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.race.EngineKVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.race.Util;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

/**
 * @author chenyuxi
 * @since 19-7-30:下午4:34
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class EngineTest {
    String path = "test/data";
    @Test
    public void smallWrite() throws KVSException{
        EngineKVStoreRace engineKVStoreRace = new EngineKVStoreRace();
        engineKVStoreRace.init(path,0);
        long key = 236223615021L;
        //获取当前堆的大小 byte 单位
//        long heapSize = Runtime.getRuntime().totalMemory();
//        System.out.println(heapSize);
//
//        //获取堆的最大大小byte单位
//        //超过将抛出 OutOfMemoryException
//        long heapMaxSize = Runtime.getRuntime().maxMemory();
//        System.out.println(heapMaxSize);
//
//        //获取当前空闲的内存容量byte单位
//        long heapFreeSize = Runtime.getRuntime().freeMemory();
//        System.out.println(heapFreeSize);
        long errorkey1 = 236223616977L;
        for (long i = 249108103217L; i <249108146717L ; i++) {
            long finalI = i;
            String finalS = String.valueOf(i);
            byte[] value = Util.genvalue(finalI);
            engineKVStoreRace.set(finalS, value);
//            if(i == 249108133217L ){
//                String name = ManagementFactory.getRuntimeMXBean().getName();
//                System.out.println(name);
//                String pid = name.split("@")[0];
//                Util.closeLinuxProcess(pid);
//            }
        }
    }
    @Test
    public void smallReadTest() throws KVSException{
        EngineKVStoreRace engineKVStoreRace = new EngineKVStoreRace();
        engineKVStoreRace.init(path,0);
        Ref<byte[]> value = new Ref<>(null);

        for (long i = 249108125218L; i < 249108125219L; i++) {
            long finalI = i;
            String finalS = String.valueOf(finalI);
            engineKVStoreRace.get(finalS,value);
            byte[] valueByte = value.getValue();
            byte[] valueByteGen = Util.genvalue(finalI);
            System.out.println("valueByte[]:"+ Arrays.toString(valueByte));
            System.out.println("------------");
            System.out.println("valueByteGen[]:"+ Arrays.toString(valueByteGen));

        }
    }
    @Test
    public void pidTest() throws Exception{
        String name = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(name);
        String pid = name.split("@")[0];
        System.out.println("Pid is:" + pid);

    }
}
