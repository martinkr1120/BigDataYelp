package edu.neu.info7250.review_classification;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<CompositeKeyWritable, NullWritable> {

	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value, int numPartitions) {
		return (key.getWord().hashCode() % numPartitions);
	}

}
