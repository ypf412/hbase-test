package ypf412.hbase.test.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ypf412.hbase.test.worker.Constants.Write;

/**
 * HBase Test Writer Class: Single Column Qualifier || Multiple Column Qualifiers
 * 
 * @author jiuling.ypf
 * 
 */
public class HBaseWriter {
	
	private static final Log LOG = LogFactory.getLog(HBaseWriter.class);
	
	private static final Configuration conf = HBaseConfiguration.create();
	
	private HTable[] hTables;
	
	private String dataDir = "/home/lz/tt/DATA/RAWED/taobao_acookie";
	
	private String fieldSpliter = "";
	
	private int threadNum = 1;
	
	private String tableName = "test";
	
	private String columnFamily = "t";
	
	private Write writeType;
	
	public HBaseWriter(String dataDir, String fieldSpliter, int threadNum, String tableName, String columnFamily, Write writeType) {
		this.dataDir = dataDir;
		this.fieldSpliter = fieldSpliter;
		this.threadNum = threadNum;
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		this.writeType = writeType;
		
		initHTables();
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
				hTables[i].setAutoFlush(true);
			}
		} catch (IOException e) {
			LOG.error("initHTables error, e=" + e.getStackTrace());
			return false;
		}
		return true;
	}
	
	/**
	 * start writer workers
	 */
	public void startWorkers() {
		LOG.info("---------- HBase writer begin to work ----------");
		CountDownLatch countDownLatch = new CountDownLatch(threadNum);
		try {
			for (int shard = 0; shard < threadNum; shard++) {
				Thread shardThread = new Thread(new ShardWrite(shard, countDownLatch));
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
		LOG.info("---------- HBase writer finish to work ----------");
	}
	
	/**
	 * thread for every shard
	 * @author jiuling.ypf
	 *
	 */
	private class ShardWrite implements Runnable {
		
		private int shardNum;
		private CountDownLatch countDownLatch;
		private List<File> fileList;


		public ShardWrite(int shardNum, CountDownLatch countDownLatch) throws Exception {
			this.shardNum = shardNum;
			this.countDownLatch = countDownLatch;
			this.fileList = loadFiles();
		}

		public void run() {
			int lineNum = 0;
			for (File file : fileList) {
				FileReader r = null;
				try {
					r = new FileReader(file);
				} catch (FileNotFoundException e1) {
					LOG.error("file not found", e1);
				}
				final BufferedReader br = new BufferedReader(r);
				String line = null;
				try {
					line = br.readLine();
				} catch (IOException e) {
					LOG.error("file read error", e);
				}
				lineNum = 0;
				while (line != null) {
					lineNum++;
					
					if (line.length() == 0)
						continue;
					
					byte[] rowKey = getRowKeyForPut(shardNum, file, lineNum);
					Counter.addDataLen(rowKey.length, Bytes.toBytes(line).length);
					Put put = null;
					if (writeType == Write.SINGLE_COLUMN) {
						put = getPutForSingleColumn(rowKey, line);
					} else if (writeType == Write.MULTIPLE_COLUMNS) {
						put = getPutForMultipleColumns(rowKey, line);
					}
					writeToHBaseDB(put);
					try {
						line = br.readLine();
					} catch (IOException e) {
						LOG.error("file read error", e);
					}
				}
				try {
					br.close();
					r.close();
				} catch (IOException e) {
					LOG.error("file close error", e);
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

		private byte[] getRowKeyForPut(int shardNum, File file, int lineNum) {
			byte[] bShard = Bytes.toBytes(shardNum);
			byte[] bFile = Bytes.toBytes(file.getAbsolutePath());
			byte[] bLine = Bytes.toBytes(lineNum);
			byte[] rowKey = Bytes.add(bShard, bFile, bLine);
			return rowKey;
		}

		private Put getPutForSingleColumn(byte[] rowKey, String line) {
			Put put = new Put(rowKey);
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("col"), Bytes.toBytes(line));
			return put;
		}

		private Put getPutForMultipleColumns(byte[] rowKey, String line) {
			Put put = new Put(rowKey);
			String[] data = line.split(fieldSpliter);
			for (int i = 0; i < data.length; i++) {
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("col" + i), Bytes.toBytes(data[i]));
			}
			return put;
		}
		
		private void writeToHBaseDB(Put put) {
			long start = System.currentTimeMillis();
			try {
				hTables[shardNum].put(put);
				Counter.addSuccess((System.currentTimeMillis() - start));
			} catch (IOException e) {
				Counter.addFailure((System.currentTimeMillis() - start));
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
		private static long recordNum;
		
		// process total time
		private static long totalTime;
		
		private static long totalKeyLen;
		
		private static long totalValLen;
		
		// < 1 ms
		private static long countA;
		
		// 1 ~ 2ms
		private static long countB;
		
		// 2 ~ 10 ms 
		private static long countC;
		
		// 10 ~ 20 ms
		private static long countD;
		
		// > 20 ms
		private static long countE;
		
		// success count
		private static long countSucess;
		
		// failure count
		private static long countFailure;
		
		private static synchronized void count(long t) {
			recordNum++;
			totalTime += t;
			
			if(t < 1){
				countA++;
			}
			else if(t < 2){
				countB++;
			}
			else if(t< 10){
				countC++;
			}
			else if(t<20){
				countD++;
			}
			else{
				countE++;
			}
		}
		
		public static synchronized void addSuccess(long t) {
			count(t);
			countSucess++;
		}
		
		public static synchronized void addFailure(long t) {
			count(t);
			countFailure++;
		}
		
		public static void addDataLen(int keyLen, int valLen) {
			totalKeyLen += keyLen;
			totalValLen += valLen;
		}
		
		// print statistic log
		public static void printStat(){
			System.out.println("----------------------- stat report ---------------------------------");
			System.out.println("totalLine = " + recordNum + ", totalTime = " + totalTime);
			System.out.println("countSucess = " + countSucess + ", countFailure = " + countFailure);
			System.out.println("totalKeyLen = " + totalKeyLen + ", totalValLen = " + totalValLen);
			if(recordNum != 0) {
				System.out.println("average time per line = " + (totalTime / recordNum));
				System.out.println("average len per key = " + (totalKeyLen / recordNum));
				System.out.println("average len per val = " + (totalValLen / recordNum));
			}
			System.out.println("< 1ms = " + countA + ", 1~2 ms = " + countB
					+ ", 2~10 ms = " + countC + ", 10~20ms = " + countD + ", > 20ms = " + countE);
			System.out.println("----------------------- stat report ---------------------------------");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String args[]) {
		if (args.length != 6) {
			System.err
					.println("Usage: ypf412.hbase.test.worker.HBaseWriter [dataDir] [fieldSpliter] [threadNum] [tableName] [columnFamily] [writeType(single|multiple)]");
			System.exit(1);
		}
		String dataDir = args[0];
		String fieldSpliter = args[1];
		int threadNum = Integer.parseInt(args[2]);
		String tableName = args[3];
		String columnFamily = args[4];
		Write writeType = Write.SINGLE_COLUMN;
		if (args[5].equalsIgnoreCase("single"))
			writeType = Write.SINGLE_COLUMN;
		else if (args[5].equalsIgnoreCase("multiple"))
			writeType = Write.MULTIPLE_COLUMNS;
		else {
			System.err.println("invalid write type: " + args[5]);
			System.exit(1);
		}
		HBaseWriter writer = new HBaseWriter(dataDir, fieldSpliter, threadNum, tableName, columnFamily, writeType);
		writer.startWorkers();	
	}
	
}
