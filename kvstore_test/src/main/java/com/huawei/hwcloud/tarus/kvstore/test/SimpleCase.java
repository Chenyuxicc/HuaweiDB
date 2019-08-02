package com.huawei.hwcloud.tarus.kvstore.test;

import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.race.EngineKVStoreRace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SimpleCase {

    private static final Logger log = LoggerFactory.getLogger(SimpleCase.class);

    public void test(final String dir, final int times) {
        int threadNums = 16;

        KVStoreRace[] racers = new KVStoreRace[threadNums];
        for (int i = 0; i < threadNums; i++) {
            racers[i] = new EngineKVStoreRace();
            racers[i].init(dir, i);
        }

        Thread[] threads = new Thread[threadNums];
        for (int i = 0; i < threadNums; i++) {
            final int threadNum = i;
            threads[i] = new Thread(() -> {
                write(racers[threadNum], times);
                read(racers[threadNum], times);
                racers[threadNum].close();
            });
            threads[i].start();
        }
        for (int i = 0; i < threadNums; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


    public void write(KVStoreRace racer, final int times) {
        for (int i = 0; i < times; i++) {
            String key = buildKey(i + 1);
            byte[] val = buildVal(i + 1);
            racer.set(key, val);
        }

        for (int i = 0; i < times; i++) {
            String key = buildKey(i + 1);
            byte[] val = buildVal(i + 1);
            Ref<byte[]> val_ref = Ref.of(byte[].class);
            racer.get(key, val_ref);
            if (!Arrays.equals(val_ref.getValue(), val)) {
                log.error("hhhhhget key=[{}] error, val size=[{}]", i, val.length);
                break;
            }
        }
    }

    public void read(KVStoreRace racer, final int times) {
        for (int i = 0; i < times; i++) {
            String key = buildKey(i + 1);
            byte[] val = buildVal(i + 1);
            Ref<byte[]> val_ref = Ref.of(byte[].class);
            racer.get(key, val_ref);
            if (!Arrays.equals(val_ref.getValue(), val)) {
                log.error("jjjjjjget key=[{}] error, val size=[{}]", i, val.length);
                break;
            }
        }

    }

    private final String buildKey(final int i) {
        return i + "";
    }

    private final byte[] buildVal(final int i) {
        byte[] bytes;
        bytes = Util.long2bytes((long) i);
        ByteBuffer allocate = ByteBuffer.allocate(4 * 1024);
        allocate.put(bytes);
        int remaining = allocate.remaining();
        for (int j = 0; j < remaining; j++) {
            allocate.put((byte) 0);
        }
        allocate.flip();
        return allocate.array();
    }
}
