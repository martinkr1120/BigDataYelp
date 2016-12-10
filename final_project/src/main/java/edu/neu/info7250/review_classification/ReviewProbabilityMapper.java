package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewProbabilityMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String[] strArr = line.split(",");

		try {
			context.write(new Text(strArr[3]),
					new Text(new StringBuilder().append(strArr[1]).append(",").append(strArr[2]).toString()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
