package ypf412.hbase.test.analysis;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutHeapSize {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// single column Put size
		byte[] rowKey = new byte[64];
		byte[] value = new byte[751];
		Put singleColumnPut = new Put(rowKey);
		singleColumnPut.add(Bytes.toBytes("t"), Bytes.toBytes("col"), value);
		System.out.println("single column Put size: " + singleColumnPut.heapSize());
		
		// multiple columns Put size
		value = null;
		Put multipleColumnsPut = new Put(rowKey);
		for (int i = 0; i < 53; i++) {
			multipleColumnsPut.add(Bytes.toBytes("t"), Bytes.toBytes("col" + i), value);
		}
		System.out.println("multiple columns Put size: " + (multipleColumnsPut.heapSize() + 751));
	}
}
