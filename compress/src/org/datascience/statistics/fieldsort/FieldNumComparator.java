package org.datascience.statistics.fieldsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FieldNumComparator extends WritableComparator{
	protected FieldNumComparator() {
		super(FieldNumKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FieldNumKey f1 = (FieldNumKey)w1;
		FieldNumKey f2 = (FieldNumKey)w2;
		
		int cmp = f1.getField().compareTo(f2.getField());
		if (cmp != 0)
			return cmp;
		return f2.getNum() - f1.getNum();
	}
}
