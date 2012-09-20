package ypf412.hbase.test;

import java.util.Arrays;

import ypf412.hbase.test.table.CreateTable;
import ypf412.hbase.test.table.DropTable;
import ypf412.hbase.test.worker.HBaseReader;
import ypf412.hbase.test.worker.HBaseWriter;

/**
 * Main Class
 * 
 * @author jiuling.ypf
 * 
 */
public class HBaseTest {
	
	private static final String[] allowedOperations = {"createTable", "putTable", "getTable", "scanTable", "dropTable"};

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Invalid number of parameters: at least 2 parameters!");
			System.out.println("Usage: ");
			System.out
					.println("       ypf412.hbase.test.HBaseTest [operation] [parameters...]");
			System.out
					.println("       [operation]: createTable, putTable, getTable, scanTable, dropTable");
			System.out.println("Example: ");
			System.out
					.println("       ypf412.hbase.test.HBaseTest createTable test t 256");
			System.out
					.println("       ypf412.hbase.test.HBaseTest dropTable test");
			System.exit(1);
		}

		Arrays.sort(allowedOperations);
		String operation = args[0];
		int index = Arrays.binarySearch(allowedOperations, operation);
		if (index >= 0) {
			String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
			if ("createTable".equals(operation))
				CreateTable.main(newArgs);
			else if ("putTable".equals(operation))
				HBaseWriter.main(newArgs);
			else if ("getTable".equals(operation) || "scanTable".equals(operation))
				HBaseReader.main(newArgs);
			else if ("dropTable".equals(operation))
				DropTable.main(newArgs);
		} else {
			System.err.println("invalid operation: " + operation);
			System.exit(1);
		}
	}

}
