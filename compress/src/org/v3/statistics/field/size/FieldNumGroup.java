package org.v3.statistics.field.size;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldNumGroup extends WritableComparator{
	protected FieldNumGroup() {
		super(FieldSizeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldSizeKey f1 = (FieldSizeKey)w1;
		FieldSizeKey f2 = (FieldSizeKey)w2;
		
		return f1.getField().compareTo(f2.getField());
	}
}
