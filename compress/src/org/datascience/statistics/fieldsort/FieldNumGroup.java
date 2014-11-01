package org.datascience.statistics.fieldsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldNumGroup extends WritableComparator{
	protected FieldNumGroup() {
		super(FieldNumKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldNumKey f1 = (FieldNumKey)w1;
		FieldNumKey f2 = (FieldNumKey)w2;
		
		return f1.getField().compareTo(f2.getField());
	}
}
