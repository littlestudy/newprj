package org.v1.datascience.statistics.fieldsizesort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FieldPartitioner extends Partitioner<FieldSizeKey, Text>{

	@Override
	public int getPartition(FieldSizeKey key, Text value, int numPartitions) {
		return Math.abs(key.getField().hashCode() * 127) % numPartitions;
	}

}
