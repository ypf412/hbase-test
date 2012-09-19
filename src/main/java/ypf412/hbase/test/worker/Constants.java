package ypf412.hbase.test.worker;

/**
 * Constants Interface
 * 
 * @author jiuling.ypf
 *
 */
public interface Constants {

	public enum Write {
		PUT_TO_SINGLE_COLUMN, 
		PUT_TO_MULTIPLE_COLUMNS
	};

	public enum Read {
		SCAN_BY_COLUMN_FAMILY, 
		GET_BY_COLUMN_FAMILY, 
		SCAN_BY_COLUMN, 
		GET_BY_COLUMN
	};

}
