package ypf412.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import ypf412.hbase.test.table.CreateTable;
import ypf412.hbase.test.table.DropTable;

/**
 * Main Class
 * 
 * @author jiuling.ypf
 * 
 */
public class HBaseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Invalid number of parameters: must be >= 2 parameters!");
			System.out.println("Usage: ");
			System.out
					.println("       ypf412.hbase.test.HBaseTest [operation] [parameter...]");
			System.out
					.println("       [operation]: createTable, putTable, getTable, scanTable, dropTable");
			System.out.println("Example: ");
			System.out
					.println("       ypf412.hbase.test.HBaseTest createTable test t 256");
			System.out
					.println("       ypf412.hbase.test.HBaseTest dropTable test");
			System.exit(1);
		}

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}

		String operation = args[0];
		if (operation.equalsIgnoreCase("createTable")) {
			CreateTable createTable = new CreateTable(admin);
			if (args.length != 4) {
				System.out
						.println("Invalid number of parameters: must be 4 when operation = createTable");
				System.exit(1);
			}
			String tableName = args[1];
			String columnFamily = args[2];
			short preRegionNumber = Short.parseShort(args[3]);
			createTable.doWork(tableName, columnFamily, preRegionNumber);
		} else if (operation.equalsIgnoreCase("dropTable")) {
			DropTable dropTable = new DropTable(admin);
			if (args.length != 2) {
				System.out
						.println("Invalid number of parameters: must be 2 when operation = dropTable");
				System.exit(1);
			}
			String tableName = args[1];
			dropTable.doWork(tableName);
		} else if (operation.equalsIgnoreCase("***")) {

		} else {
			System.err.println("Invalid operation: " + operation
					+ ", terminate program");
		}
	}

}
