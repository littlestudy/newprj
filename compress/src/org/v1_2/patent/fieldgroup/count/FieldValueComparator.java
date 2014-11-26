package org.v1_2.patent.fieldgroup.count;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldValueComparator extends WritableComparator{
	
	protected FieldValueComparator(){		
		super(FieldGroupValueKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldGroupValueKey f1 = (FieldGroupValueKey)w1;
		FieldGroupValueKey f2 = (FieldGroupValueKey)w2;
		
		int cmp = f1.getField().compareTo(f2.getField());
		if (cmp != 0)
			return cmp;
		return f1.getValue().compareTo(f2.getValue());
	}
}
