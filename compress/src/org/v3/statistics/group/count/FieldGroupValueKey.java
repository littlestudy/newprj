package org.v3.statistics.group.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FieldGroupValueKey implements WritableComparable<FieldGroupValueKey>{

	private String fieldGroup;
	private String value;
	
	public FieldGroupValueKey(){		
	}
	
	public String getField() {
		return fieldGroup;
	}

	public void setField(String field) {
		this.fieldGroup = field;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(fieldGroup);
		out.writeUTF(value);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.fieldGroup = in.readUTF();
		this.value = in.readUTF();		
	}

	@Override
	public int compareTo(FieldGroupValueKey o) {
		int cmp = this.fieldGroup.compareTo(o.fieldGroup);
		if (cmp != 0)
			return cmp;
		
		return this.value.compareTo(o.value);
	}

	public void set(String field, String value){
		this.fieldGroup = field;
		this.value = value;
	}

	@Override
	public String toString() {
		return fieldGroup + "\t" + value.trim();
	}	
}
