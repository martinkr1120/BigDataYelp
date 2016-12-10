package edu.neu.info7250.review_classification;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeySortComparator extends WritableComparator {
	
	public SecondarySortCompKeySortComparator(){
		super(CompositeKeyWritable.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2){
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		
		int cmpResult = key1.getWord().compareTo(key2.getWord());
		if(cmpResult == 0){
			return -(key1.getCount_or_rid().length() - key2.getCount_or_rid().length());
		}
		return cmpResult;
	}
}
