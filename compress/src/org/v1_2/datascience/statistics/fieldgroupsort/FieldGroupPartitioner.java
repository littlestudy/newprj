package org.v1_2.datascience.statistics.fieldgroupsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FieldGroupPartitioner extends Partitioner<FieldGroupSizeKey, Text>{

	@Override
	public int getPartition(FieldGroupSizeKey key, Text value, int numPartitions) {
		return Math.abs(key.getFieldGroup().hashCode() * 127) % numPartitions;
	}

}
