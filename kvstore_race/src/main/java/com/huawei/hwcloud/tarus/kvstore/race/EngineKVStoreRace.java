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
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.util.concurrent.FastThreadLocal;

public class EngineKVStoreRace implements KVStoreRace {
    private String dir;
	private static Logger logger = LoggerFactory.getLogger(EngineKVStoreRace.class);
    private static final int VALUE_LEN = 4096;
    private static final int KEY_AND_OFFSET = 12;

    //分成16个value文件
    private static final int VALUE_FILE_COUNT  = 16;
    private static final int SHIFT_NUM = 12;


    private  FileChannel[] fileChannels = new FileChannel[VALUE_FILE_COUNT];
    private  AtomicInteger[] valueOffsets = new AtomicInteger[VALUE_FILE_COUNT];
    private  FileChannel keyChannel = null;
	private  AtomicInteger keyOffset = null;

	private static int WRITE_TIME = 0;
	private static int READ_TIME = 0;


	//每个线程维护自己的map.map中存放key和value在自己文件中的偏移值，偏移指的是该value在文件中的位置，是第几个value
	private  LongIntHashMap map = new LongIntHashMap(4500000,0.99);

	private FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(VALUE_LEN);
		}
	};
	private FastThreadLocal<ByteBuffer> localBufferKeyOff = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(KEY_AND_OFFSET);
		}
	};
	private FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>(){
		@Override
		protected byte[] initialValue() throws Exception{
			return new byte[VALUE_LEN];
		}
	};

	private  int threadId = -1;
    @Override
	public boolean init(final String dir, final int file_size) throws KVSException {
		logger.info("---------------init--------------");
		this.threadId = file_size;
		File file = new File(dir);
		String path = file.getParent();

		File parent = new File(path);
		this.dir = path+"/data_"+threadId;
		File fileData = new File(this.dir);
		if(!fileData.exists()){
			fileData.mkdirs();
		}
		RandomAccessFile randomAccessFile;
		try {
			String threadName = Thread.currentThread().getName();
			logger.info("---------threadname&&threadid------------------");
			logger.info("threadName={}",threadName);
			logger.info("threadid={}",this.threadId);
			logger.info("maphashcode={}",this.map.hashCode());
			logger.info("---------threadname&&threadid------------------");

			//初始化file文件
			for (int i = 0; i < VALUE_FILE_COUNT ; i++) {
				randomAccessFile = new RandomAccessFile(this.dir + File.separator + i + "_threadId_"+this.threadId+".data" , "rw");
				FileChannel fileChannel = randomAccessFile.getChannel();
				fileChannels[i] = fileChannel;
				valueOffsets[i] = new AtomicInteger((int) (randomAccessFile.length() >>> SHIFT_NUM));
				logger.info("--------------value count---------");
                logger.info("valueOffsets[i]:{}",valueOffsets[i].get());
                logger.info("--------------value count---------");
			}
			randomAccessFile = new RandomAccessFile(this.dir + File.separator + "threadId_"+ this.threadId + "_keyoff.key","rw");
			keyChannel = randomAccessFile.getChannel();
			keyOffset = new AtomicInteger((int)randomAccessFile.length() / KEY_AND_OFFSET);
            logger.info("--------------value count---------");
            logger.info("keyOffsets:{}",keyOffset.get());
            logger.info("--------------value count---------");
			int start = 0;
			int end = keyOffset.get() * KEY_AND_OFFSET;
			//先把keyChannel的position置为0
			keyChannel.position(0);
			while (start < end){
				//获取localBuffer，避免重复创建buffer，浪费资源
				ByteBuffer buffer = localBufferKeyOff.get();
				buffer.clear();
				//从start的位置读取keyChannel的内容到buffer中
				keyChannel.read(buffer,start);
				//修改buffer的位置
				buffer.position(0);
				//从buffer中读取key和off的值
				long key = buffer.getLong();
				int off = buffer.getInt();
				//改变keyChannel的读取位置
				start += KEY_AND_OFFSET;
				//将key和off放入索引的map中
				map.put(key,off);

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

    	final long numKey = Long.parseLong(key);
		final int fileHash = valueFileHash(numKey);
		int off;
		try{
			//将value的值放入buffer中
			ByteBuffer buffer = localBufferValue.get();
			buffer.clear();
			buffer.put(value);
			buffer.position(0);
			//从map中获取value的偏移量
			off = map.getOrDefault(numKey,-1);
			if(off != -1){
				//如果这个数已经存在，就将他放在原来的位置上
				fileChannels[fileHash].write(buffer , off << SHIFT_NUM);
			}else {
				//如果这个value不存在，获取当前value的个数，并将value个数自增1
				off = valueOffsets[fileHash].getAndIncrement();
				//将key off写入索引map
				map.put(numKey, off);
//				logger.info("numkey={},off={}",numKey,off);

				//将key off持久化
				ByteBuffer keyOffBuffer = localBufferKeyOff.get() ;
				keyOffBuffer.clear();
				keyOffBuffer.putLong(numKey).putInt(off);
				//设置keyChannel的读写位置，将keybuffer的内容读取到keychannel中
				keyChannel.position(keyOffset.getAndAdd(1)*KEY_AND_OFFSET);
				keyOffBuffer.position(0);
				keyChannel.write(keyOffBuffer);

				if(WRITE_TIME == 1 || WRITE_TIME == 300000){
					logger.info("-----numkey={},off={},value={}-----",numKey,off,value);
				}
				//将value持久化
				fileChannels[fileHash].write(buffer, off << SHIFT_NUM);
			}
			return off >> 32;
		}catch (Exception e){
			e.printStackTrace();
			throw new KVSException("写入数据出错");
		}
	}
	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {
    	READ_TIME++;
		final long numKey = Long.parseLong(key);
		final int fileHash = valueFileHash(numKey);
		int off = map.getOrDefault(numKey,-1);
//		logger.info("--------------off={},numkey={}----------",off,numKey);
		if(off == -1){
			logger.info("readnum={}",numKey);
			logger.info("off=-1");
			val.setValue(null);
			return -1;
		}
		byte[] bytes = localValueBytes.get();
		try {
			ByteBuffer buffer = localBufferValue.get();
			buffer.clear();
			//从filechannels中读取数据到buffer中
			fileChannels[fileHash].read(buffer,(long) off << SHIFT_NUM);
			//从buffer中读取数据到bytes中
			buffer.position(0);
			buffer.get(bytes,0,VALUE_LEN);
			//将数组设置为value的值
//			logger.info("--------------off={},numkey={},fileHash={},value={},threadNum={}----------",off,numKey,fileHash,Util.bytes2long(bytes),this.threadId);
			val.setValue(bytes);
			bytes = val.getValue();
			if(READ_TIME == 1 || READ_TIME == 300000){
				logger.info("-----numkey={},off={},value={}-----",numKey,off,bytes);
			}
			buffer.clear();
			return bytes2long(bytes);
		}catch (Exception e){
			e.printStackTrace();
			throw new KVSException("read 出错");
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
	//分成16个value文件
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
