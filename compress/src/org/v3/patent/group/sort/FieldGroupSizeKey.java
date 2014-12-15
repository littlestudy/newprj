package org.v3.patent.group.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FieldGroupSizeKey implements WritableComparable<FieldGroupSizeKey> {

	private String fieldGroup;
	private long size;

	public FieldGroupSizeKey() {
	}

	public String getFieldGroup() {
		return fieldGroup;
	}

	public void setFieldGroup(String field) {
		this.fieldGroup = field;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(fieldGroup);
		out.writeLong(size);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.fieldGroup = in.readUTF();
		this.size = in.readLong();
	}

	@Override
	public int compareTo(FieldGroupSizeKey o) {
		int cmp = this.fieldGroup.compareTo(o.fieldGroup);
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
		this.fieldGroup = field;
		this.size = size;
	}

	@Override
	public String toString() {
		return fieldGroup + "\t" + size ;
	}	
}
