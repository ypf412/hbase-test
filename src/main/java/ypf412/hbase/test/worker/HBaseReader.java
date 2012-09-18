package ypf412.hbase.test.worker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ypf412.hbase.test.worker.Constants.Read;

/**
 * HBase Test Writer Class: Scan || Get
 * 
 * @author jiuling.ypf
 *
 */
public class HBaseReader {
	
	private static final Log LOG = LogFactory.getLog(HBaseReader.class);
	
	private static final Configuration conf = HBaseConfiguration.create();
	
	private HTable[] hTables;
	
	private String dataDir = "/home/lz/tt/DATA/RAWED/taobao_acookie";
	
	private int threadNum = 1;
	
	private String tableName = "test";
	
	private String columnFamily = "t";
	
	private Read readType;
	
	public HBaseReader(String dataDir, int threadNum, String tableName, String columnFamily, Read readType) {
		this.dataDir = dataDir;
		this.threadNum = threadNum;
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		this.readType = readType;
		
		initHTables();
		startWorkers();
	}
	
	/**
	 * init HBase tables for every thread
	 * 
	 * @return
	 */
	private boolean initHTables() {
		try {
			hTables = new HTable[threadNum];
			for (int i = 0; i < threadNum; i++) {
				hTables[i] = new HTable(conf, tableName);
			}
		} catch (IOException e) {
			LOG.error("initHTables error, e=" + e.getStackTrace());
			return false;
		}
		return true;
	}
	
	public void startWorkers() {
		LOG.info("---------- HBase reader begin to work ----------");
		CountDownLatch countDownLatch = new CountDownLatch(threadNum);
		try {
			for (int shard = 0; shard < threadNum; shard++) {
				Thread shardThread = new Thread(new ShardRead(shard, countDownLatch));
				shardThread.setDaemon(true);
				shardThread.start();
			}
		} catch (Exception e) {
			LOG.error("failed to create shard thread", e);
			throw new RuntimeException(e);
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			LOG.error("worker thread is interrupted", e);
		}
		Counter.printStat();
		LOG.info("---------- HBase reader finish to work ----------");
	}
	
	private class ShardRead implements Runnable {
		
		private int shardNum;
		private CountDownLatch countDownLatch;
		private List<File> fileList;
		private Random rand = new Random();


		public ShardRead(int shardNum, CountDownLatch countDownLatch) throws Exception {
			this.shardNum = shardNum;
			this.countDownLatch = countDownLatch;
			this.fileList = loadFiles();
		}

		public void run() {
			for (File file : fileList) {
				if (readType == Read.SCAN) {
					scanFromHBaseDB(shardNum, file);
				} else if (readType == Read.GET) {
					getFromHBaseDB(shardNum, file);
				}
			}
			countDownLatch.countDown();
		}
		
		private List<File> loadFiles() {
			List<File> fileList = new ArrayList<File>();
			File dir = new File(dataDir);
			if (dir.exists() && dir.isDirectory()) {
				File[] allFiles = dir.listFiles();
				for (int i=0; i<allFiles.length; i++) {
					if (i % threadNum == shardNum)
						fileList.add(allFiles[i]);
				}
			} else {
				LOG.error("parameter dataDir: " + dataDir + " is not a directory");
			}
			return fileList;
		}

		private void scanFromHBaseDB(int shardNum, File file) {
			long start = System.currentTimeMillis();
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(columnFamily));
			scan.setCaching(0);
			byte[] bShard = { (byte)shardNum };
			byte[] startRow = Bytes.add(bShard, Bytes.toBytes(file.getAbsolutePath()));
			byte[] bShardN = { (byte)(shardNum + 1) };
			byte[] stopRow = bShardN;
			scan.setStartRow(startRow);
			scan.setStopRow(stopRow);
			ResultScanner rs = null;
			try {
				rs = hTables[shardNum].getScanner(scan);
			} catch (IOException e) {
				LOG.error("get scanner error", e);
			}
			Result rr = new Result();
			while (rr != null) {
				try {
					rr = rs.next();
				} catch (IOException e) {
					LOG.error("result scanner next error", e);
				}
				if (rr != null && !rr.isEmpty()) {
					Counter.add(System.currentTimeMillis() - start);
				}
			}
			rs.close();
			
		}

		private byte[] getRowKeyForGet(int shardNum, File file) {
			int lineNum = rand.nextInt();
			byte[] bShard = { (byte)shardNum };
			byte[] bFile = Bytes.toBytes(file.getAbsolutePath());
			byte[] bLine = Bytes.toBytes(lineNum);
			byte[] rowKey = Bytes.add(bShard, bFile, bLine);
			return rowKey;
		}
		
		private void getFromHBaseDB(int shardNum, File file) {
			final int lineNumPerFile = 100000;
			long start = System.currentTimeMillis();
			for (int i = 0; i < lineNumPerFile; i++) {
				byte[] rowKey = getRowKeyForGet(shardNum, file);
				Get get = new Get(rowKey);
				get.addFamily(Bytes.toBytes(columnFamily));
				try {
					Result result = hTables[shardNum].get(get);
					if (result != null && !result.isEmpty())
						Counter.add((System.currentTimeMillis() - start));
					else
						Counter.addN((System.currentTimeMillis() - start));
				} catch (IOException e) {
					Counter.addN((System.currentTimeMillis() - start));
				}
			}
		}
		
	}
	
	/**
	 * static Counter class
	 * @author jiuling.ypf
	 *
	 */
	static class Counter{
		// process record number
		public static long recordNum;
		
		// process total time
		public static long totalTime;
		
		// process record number
		public static long recordNumN;
		
		// process total time
		public static long totalTimeN;
		
		// < 1 ms
		public static long countA;
		
		// 1 ~ 2ms
		public static long countB;
		
		// 2 ~ 10 ms 
		public static long countC;
		
		// 10 ~ 20 ms
		public static long countD;
		
		// > 20 ms
		public static long countE;

		// < 1 ms
		public static long countAN;
		
		// 1 ~ 2ms
		public static long countBN;
		
		// 2 ~ 10 ms 
		public static long countCN;
		
		// 10 ~ 20 ms
		public static long countDN;
		
		// > 20 ms
		public static long countEN;

		
		// success count
		public static long countSucess;
		
		// null result count
		public static long countNull;
		
		// put time
		public static synchronized void add(long t){
			recordNum ++ ;
			totalTime += t;
			
			if(t < 1){
				countA ++;
			}
			else if(t < 2){
				countB ++;
			}
			else if(t< 10){
				countC ++;
			}
			else if(t<20){
				countD ++;
			}
			else{
				countE ++ ;
			}
			
			countSucess ++;
		}
		
		// put time
		public static synchronized void addN(long t){
			recordNumN ++ ;
			totalTimeN += t;
			
			if(t < 1){
				countAN ++;
			}
			else if(t < 2){
				countBN ++;
			}
			else if(t< 10){
				countCN ++;
			}
			else if(t<20){
				countDN ++;
			}
			else{
				countEN ++ ;
			}
			
			countNull ++ ;
		}
		
		// print statistic log
		public static synchronized void printStat(){
			System.out.println("----------------------- success stat---------------------------------");
			System.out.println("readlines = " + recordNum + ", totalTime = " + totalTime + ", hit="+countSucess);
			if(recordNum != 0){
				System.out.println("average time per line = "
						+ (totalTime / recordNum));	
			}
			System.out.println("< 1ms = " + countA + ", 1~2 ms = " + countB
					+ ", 2~10 ms = " + countC + ", 10~20ms = " + countD+ ", > 20ms = " + countE);
			System.out.println("----------------------- success stat---------------------------------");
			
			System.out.println("----------------------- null stat---------------------------------");
			System.out.println("readlines = " + recordNumN + ", totalTime = " + totalTimeN + ", hit="+countNull);
			if(recordNumN != 0){
				System.out.println("average time per line = "
						+ (totalTimeN / recordNumN));
			}
			System.out.println("< 1ms = " + countAN + ", 1~2 ms = " + countBN
					+ ", 2~10 ms = " + countCN + ", 10~20ms = " + countDN+ ", > 20ms = " + countEN);
			System.out.println("----------------------- null stat---------------------------------");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 5) {
			System.err
					.println("Usage: ypf412.hbase.test.worker.HBaseReader [dataDir] [threadNum] [tableName] [columnFamily] [readType(scan|get)]");
			System.exit(1);
		}
		
		String dataDir = args[0];
		int threadNum = Integer.parseInt(args[1]);
		String tableName = args[2];
		String columnFamily = args[3];
		Read readType = Read.SCAN;
		if (args[4].equalsIgnoreCase("scan"))
			readType = Read.SCAN;
		else if (args[4].equalsIgnoreCase("get"))
			readType = Read.GET;
		else {
			System.err.println("invalid read type: " + args[4]);
			System.exit(1);
		}
		HBaseReader reader = new HBaseReader(dataDir, threadNum, tableName, columnFamily, readType);
		reader.startWorkers();
	}

}
