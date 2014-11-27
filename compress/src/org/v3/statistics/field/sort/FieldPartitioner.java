package org.v3.statistics.field.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FieldPartitioner extends Partitioner<FieldNumKey, Text>{

	@Override
	public int getPartition(FieldNumKey key, Text value, int numPartitions) {
		return Math.abs(key.getField().hashCode() * 127) % numPartitions;
	}

}
