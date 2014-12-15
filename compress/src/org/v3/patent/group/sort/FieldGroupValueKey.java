package org.v3.patent.group.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FieldGroupValueKey implements WritableComparable<FieldGroupValueKey>{
	private String groupFields;
	private long size;
	
	public FieldGroupValueKey() {
	}	
	
	public String getGroupFields() {
		return groupFields;
	}



	public void setGroupFields(String groupFields) {
		this.groupFields = groupFields;
	}



	public long getSize() {
		return size;
	}



	public void setSize(long size) {
		this.size = size;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(groupFields);
		out.writeLong(size);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.groupFields = in.readUTF();
		this.size = in.readLong();
	}

	@Override
	public int compareTo(FieldGroupValueKey o) {
		int cmp = this.groupFields.compareTo(o.groupFields);
		if (cmp != 0)
			return cmp;
		
		if (this.size > o.size)
			return -1;
		else if (this.size == o.size)
			return 0;
		else 
			return 1;
	}

	public void set(String groupFields, long size){
		this.groupFields = groupFields;
		this.size = size;
	}

	@Override
	public String toString() {
		return groupFields + "\t" + size;
	}	
}
