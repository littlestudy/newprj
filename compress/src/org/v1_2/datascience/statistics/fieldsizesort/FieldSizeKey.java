package org.v1_2.datascience.statistics.fieldsizesort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FieldSizeKey implements WritableComparable<FieldSizeKey> {

	private String field;
	private long size;

	public FieldSizeKey() {
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(field);
		out.writeLong(size);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.field = in.readUTF();
		this.size = in.readLong();
	}

	@Override
	public int compareTo(FieldSizeKey o) {
		int cmp = this.field.compareTo(o.field);
		if (cmp != 0)
			return cmp;
		
		if (this.size > o.size)
			return -1;
		else  if (this.size == o.size)
			return 0;
		else
			return 1;
	}

	public void set(String field, long size){
		this.field = field;
		this.size = size;
	}

	@Override
	public String toString() {
		return field + "\t" + size ;
	}	
}
