package edu.neu.info7250.review_classification;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
	
	private String word;
	private String count_or_rid;
	
	public CompositeKeyWritable(){
		
	}
	
	public CompositeKeyWritable(String word, String count_or_rid){
		this.word = word;
		this.count_or_rid = count_or_rid;
	}
	
	public String toString(){
		return (new StringBuilder().append(word).append(",").append(count_or_rid)).toString();
	}
	
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, word);
		WritableUtils.writeString(out, count_or_rid);
	}

	public void readFields(DataInput in) throws IOException {
		word = WritableUtils.readString(in);
		count_or_rid = WritableUtils.readString(in);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		int result = word.compareTo(objKeyPair.word);
		if(0 == result){
			result = count_or_rid.compareTo(objKeyPair.count_or_rid);
		}
		return result;
	}
	
	public String getWord(){
		return word;
	}
	
	public void setWord(String word){
		this.word = word;
	}
	
	public String getCount_or_rid(){
		return count_or_rid;
	}
	
	public void setCount_or_rid(String count_or_rid){
		this.count_or_rid = count_or_rid;
	}

}
