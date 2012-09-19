package ypf412.hbase.test.table;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Drop HBase Table for Testing
 * 
 * @author jiuling.ypf
 * 
 */

public class DropTable {

	private static final Log LOG = LogFactory.getLog(DropTable.class);

	private static final Configuration conf = HBaseConfiguration.create();

	private HBaseAdmin admin;

	public DropTable(HBaseAdmin admin) {
		this.admin = admin;
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean doWork(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				LOG.info("table: " + tableName
						+ " has been disable and deleted");
			} else {
				LOG.error("table: " + tableName
						+ " not exists, failed to delete");
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e);
			return false;
		}
		return true;
	}

	/**
	 * 创建表入口
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		if (args.length != 1) {
			System.out
					.println("Usage: ypf412.hbase.test.table.DropTable [tableName]");
			System.exit(1);
		}
		String tableName = args[0];
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
		DropTable dropTable = new DropTable(admin);
		dropTable.doWork(tableName);
	}
}
