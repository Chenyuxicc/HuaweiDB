package com.huawei.hwcloud.tarus.kvstore.race;

import com.carrotsearch.hppc.LongIntHashMap;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.util.concurrent.FastThreadLocal;

public class EngineKVStoreRace implements KVStoreRace {
    private String dir;
	private static Logger logger = LoggerFactory.getLogger(EngineKVStoreRace.class);
    private static final int VALUE_LEN = 4096;
    private static final int KEY_AND_OFFSET = 12;

    private static final int VALUE_FILE_COUNT  = 512;
    private static final int SHIFT_NUM = 12;

    private volatile int WRITE_TIME = 0;
    private volatile int READ_TIME = 0;

    private static FileChannel[] fileChannels = new FileChannel[VALUE_FILE_COUNT];
    private static AtomicInteger[] valueOffsets = new AtomicInteger[VALUE_FILE_COUNT];
    private static FileChannel keyChannel = null;
	private static AtomicInteger keyOffset = null;

	private static LongIntHashMap map = new LongIntHashMap(64000000,0.99);

	private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(VALUE_LEN);
		}
	};
	private static FastThreadLocal<ByteBuffer> localBufferKeyOff = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(KEY_AND_OFFSET);
		}
	};
	private static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>(){
		@Override
		protected byte[] initialValue() throws Exception{
			return new byte[VALUE_LEN];
		}
	};

    @Override
	public boolean init(final String dir, final int file_size) throws KVSException {
		logger.info("---------------init--------------");
		File file = new File(dir);
		String path = file.getParent();
		this.dir = path;
		File file1 = new File(this.dir);
		System.out.println("---------dir:"+dir+"---------");
		System.out.println("---------path:"+path+"---------");
		System.out.println("---------file_size:"+file_size+"---------");
		if(!file1.exists()){
			file1.mkdirs();
		}
		RandomAccessFile randomAccessFile;
		try {
			//初始化file文件
			for (int i = 0; i < VALUE_FILE_COUNT ; i++) {
				randomAccessFile = new RandomAccessFile(this.dir + File.separator + i + ".data" , "rw");
				FileChannel fileChannel = randomAccessFile.getChannel();
				fileChannels[i] = fileChannel;
				valueOffsets[i] = new AtomicInteger((int) (randomAccessFile.length() >>> SHIFT_NUM));
			}
			randomAccessFile = new RandomAccessFile(this.dir + File.separator +   "keyoff.key","rw");
			keyChannel = randomAccessFile.getChannel();
			keyOffset = new AtomicInteger((int)randomAccessFile.length() / KEY_AND_OFFSET);
			int start = 0;
			int end = keyOffset.get() * KEY_AND_OFFSET;
			while (start < end){
				ByteBuffer buffer = ByteBuffer.allocate(KEY_AND_OFFSET);
				synchronized (KVStoreRace.class){
					keyChannel.read(buffer,start);
				}
				buffer.flip();
				long key = buffer.getLong();
				int off = buffer.getInt();
				start += KEY_AND_OFFSET;
				synchronized (KVStoreRace.class){
					map.put(key,off);
				}
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		System.out.println("----------init finished----------");
    	return true;
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {
		WRITE_TIME++;
		long numKey = Long.parseLong(key);
		int fileHash = valueFileHash(numKey);
		int off;
		try{
			ByteBuffer buffer = localBufferValue.get();
			buffer.clear();
			buffer.put(value);
			buffer.flip();
			synchronized (KVStoreRace.class){
				off = map.getOrDefault(numKey,-1);
				if(off != -1){
					fileChannels[fileHash].write(buffer , off << SHIFT_NUM);
				}else {
					//偏移就是这个value在这个filechannel的位置(第几个)
					off = valueOffsets[fileHash].getAndIncrement();
					//将key off写入索引map
					map.put(numKey, off);

					//将key off持久化
					ByteBuffer buffer1 = ByteBuffer.allocate(KEY_AND_OFFSET);
					buffer1.putLong(numKey).putInt(off);
					keyChannel.position(keyOffset.getAndAdd(1)*KEY_AND_OFFSET);
					buffer1.flip();
					keyChannel.write(buffer1);
					//将value持久化
					fileChannels[fileHash].write(buffer, off << SHIFT_NUM);
				}
			}
			return off >> 32;
		}catch (Exception e){
			e.printStackTrace();
			throw new KVSException("写入数据出错");
		}
	}
	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {
		long numKey = Long.parseLong(key);
		int fileHash = valueFileHash(numKey);
		int off = map.getOrDefault(numKey,-1);
		if(off == -1){
			System.out.println("readnum:"+numKey);
			System.out.println("off = -1");
			val.setValue(null);
			return -1;
		}
		synchronized (KVStoreRace.class){
			byte[] bytes = localValueBytes.get();
			try {
				ByteBuffer buffer = localBufferValue.get();
				fileChannels[fileHash].read(buffer,(long) off << SHIFT_NUM);
				buffer.position(0);
				buffer.get(bytes,0,VALUE_LEN);
				val.setValue(bytes);
				bytes = val.getValue();
				buffer.clear();
				return bytes2long(bytes);
			}catch (Exception e){
				e.printStackTrace();
				throw new KVSException("read 出错");
			}
		}
	}

	@Override
	public void close() {
		flush();
		for (int i = 0; i < VALUE_FILE_COUNT ; i++) {
			try {
				fileChannels[i].close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		map.clear();
        logger.info("---------close----------");
	}

	@Override
	public void flush() {
		for (int i = 0; i < VALUE_FILE_COUNT  ; i++) {
			try {
				fileChannels[i].force(true);
			}catch (IOException e){
				e.printStackTrace();
				throw new KVSException("-------filechannel flush error-----");
			}
		}
	}
	private static int valueFileHash(long key) {
    	return (int) key % VALUE_FILE_COUNT;

	}
	public static long bytes2long(byte[] bytes) {
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= (bytes[i] & 0xFF);
		}
		return result;
	}
}
