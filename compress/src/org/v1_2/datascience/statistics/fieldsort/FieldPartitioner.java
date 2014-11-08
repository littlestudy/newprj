package org.v1_2.datascience.statistics.fieldsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FieldPartitioner extends Partitioner<FieldNumKey, Text>{

	@Override
	public int getPartition(FieldNumKey key, Text value, int numPartitions) {
		return Math.abs(key.getField().hashCode() * 127) % numPartitions;
	}

}
