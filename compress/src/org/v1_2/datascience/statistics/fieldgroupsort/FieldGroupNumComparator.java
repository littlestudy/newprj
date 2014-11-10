package org.v1_2.datascience.statistics.fieldgroupsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldGroupNumComparator extends WritableComparator{
	protected FieldGroupNumComparator() {
		super(FieldGroupValueKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldGroupValueKey f1 = (FieldGroupValueKey)w1;
		FieldGroupValueKey f2 = (FieldGroupValueKey)w2;
		
		int cmp = f1.getGroupFields().compareTo(f2.getGroupFields());
		if (cmp != 0)
			return cmp;
		
		if (f1.getSize() > f2.getSize())
			return -1;
		else if (f1.getSize() == f2.getSize())
			return 0;
		else 
			return 1;
	}
}
