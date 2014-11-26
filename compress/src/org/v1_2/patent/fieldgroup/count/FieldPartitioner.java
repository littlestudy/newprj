package org.v1_2.patent.fieldgroup.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FieldPartitioner extends Partitioner<FieldGroupValueKey, IntWritable>{

	@Override
	public int getPartition(FieldGroupValueKey key, IntWritable value, int numPartitions) {
		return Math.abs(key.getField().hashCode() * 127) % numPartitions;
	}

}
