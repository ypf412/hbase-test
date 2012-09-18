package ypf412.hbase.test.table;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

/**
 * 创建测试用HBase表
 * 
 * @author jiuling.ypf
 * 
 */

public class CreateTable {

	private static final Log LOG = LogFactory.getLog(CreateTable.class);

	private static final Configuration conf = HBaseConfiguration.create();

	private HBaseAdmin admin;

	public CreateTable(HBaseAdmin admin) {
		this.admin = admin;
	}

	/**
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param preRegionNumber
	 * @return
	 */
	public boolean doWork(String tableName, String columnFamily,
			short preRegionNumber) {
		HTableDescriptor des = null;
		try {

			HColumnDescriptor disc = new HColumnDescriptor(columnFamily);
			disc.setMaxVersions(1);
			disc.setCompressionType(Algorithm.LZO); // 设置LZO压缩存储，减少磁盘IO开销，对读写性能都能有所提高
			disc.setBloomFilterType(BloomType.ROW); // 设置按行方式的BloomFilter，加快rowkey的检索过程
			disc.setDataBlockEncoding(DataBlockEncoding.DIFF); // 设置前缀压缩，减少内存开销
			disc.setTimeToLive(24 * 60 * 60); // 设置一天有效时间
			disc.setInMemory(true);

			des = new HTableDescriptor(tableName);
			des.addFamily(disc);

			byte[][] splits = new byte[preRegionNumber][];
			for (short s = 0; s < preRegionNumber; s++) {
				byte[] split = { (byte) s };
				splits[s] = split;
			}

			if (!admin.tableExists(des.getNameAsString())) { // 不存在则直接创建
				admin.createTable(des, splits);
				LOG.info("create table: " + des.getNameAsString()
						+ " sucessfully");
			} else { // 存在则重新创建
				admin.disableTable(des.getNameAsString());
				admin.deleteTable(des.getNameAsString());
				admin.createTable(des, splits);
				LOG.info("table: " + des.getNameAsString()
						+ " already exists, delete and recreate it");

			}
			return true;
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error(e);
			return false;
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error(e);
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e);
			return false;
		}
	}

	/**
	 * 创建表入口
	 * @param args
	 */
	public static void main(String args[]) {
		if (args.length != 3) {
			System.out
					.println("Usage: ypf412.hbase.test.table.CreateTable [tableName] [columnFamily] [preRegionNumber]");
			System.exit(1);
		}
		String tableName = args[0];
		String columnFamily = args[1];
		short preRegionNumber = Short.parseShort(args[2]);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error(e);
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error(e);
		}
		CreateTable createTable = new CreateTable(admin);
		createTable.doWork(tableName, columnFamily, preRegionNumber);
	}
}
