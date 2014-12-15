package org.v3.patent.group.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldGroupNumGroup extends WritableComparator{
	protected FieldGroupNumGroup() {
		super(FieldGroupValueKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldGroupValueKey f1 = (FieldGroupValueKey)w1;
		FieldGroupValueKey f2 = (FieldGroupValueKey)w2;
		
		return f1.getGroupFields().compareTo(f2.getGroupFields());
	}
}
