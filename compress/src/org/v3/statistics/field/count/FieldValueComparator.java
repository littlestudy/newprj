package org.v3.statistics.field.count;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldValueComparator extends WritableComparator{
	
	protected FieldValueComparator(){		
		super(FieldValueKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldValueKey f1 = (FieldValueKey)w1;
		FieldValueKey f2 = (FieldValueKey)w2;
		
		int cmp = f1.getField().compareTo(f2.getField());
		if (cmp != 0)
			return cmp;
		return f1.getValue().compareTo(f2.getValue());
	}
}
