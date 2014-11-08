package org.v1_2.datascience.statistics.fieldsizesort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldNumComparator extends WritableComparator{
	protected FieldNumComparator() {
		super(FieldSizeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldSizeKey f1 = (FieldSizeKey)w1;
		FieldSizeKey f2 = (FieldSizeKey)w2;
		
		int cmp = f1.getField().compareTo(f2.getField());
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
