package org.v3.statistics.field.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FieldNumKey implements WritableComparable<FieldNumKey> {

	private String field;
	private int num;

	public FieldNumKey() {
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(field);
		out.writeInt(num);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.field = in.readUTF();
		this.num = in.readInt();
	}

	@Override
	public int compareTo(FieldNumKey o) {
		int cmp = this.field.compareTo(o.field);
		if (cmp != 0)
			return cmp;
		
		return this.num - o.num;
	}

	public void set(String field, int num){
		this.field = field;
		this.num = num;
	}

	@Override
	public String toString() {
		return field + "\t" + num ;
	}	
}
