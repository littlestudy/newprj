package org.v1_2.datascience.statistics.fieldcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FieldPartitioner extends Partitioner<FieldValueKey, IntWritable>{

	@Override
	public int getPartition(FieldValueKey key, IntWritable value, int numPartitions) {
		return Math.abs(key.getField().hashCode() * 127) % numPartitions;
	}

}
