package ypf412.hbase.test.analysis;

import org.apache.hadoop.hbase.KeyValue;

public class KeyValueHeapSize {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// single column KeyValue size
		byte[] row = new byte[53]; // test row length
		byte[] family = new byte[1]; // test family length
	    byte[] qualifier = new byte[4]; // test qualifier length
	    long timestamp = 123456L; // ts
	    byte[] value = new byte[751]; // test value length
		KeyValue singleColumnKv = new KeyValue(row, family, qualifier, timestamp, value);
		System.out.println("single column KeyValue size: " + singleColumnKv.heapSize());
		
		// multiple columns KeyValue size
		value = null;
		KeyValue multipleColumnsWithoutValueKv = new KeyValue(row, family, qualifier, timestamp, value);
		System.out.println("multiple columns KeyValue size: " + (multipleColumnsWithoutValueKv.heapSize() * 53 + 751));
	}
	
}
