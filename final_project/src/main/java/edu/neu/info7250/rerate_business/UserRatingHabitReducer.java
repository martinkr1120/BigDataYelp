package edu.neu.info7250.rerate_business;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.lang.StringBuilder;
import java.util.*;

public class UserRatingHabitReducer extends MapReduceBase
    implements
      Reducer<Text, IntWritable, Text, NullWritable> {

  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output,
      Reporter reporter) throws IOException {
    

    reporter.setStatus(key.toString());
    int[] count = new int[6];
    
    while (values.hasNext()) {
      int i = values.next().get();
      count[i] += 1;
      count[0] += 1;
    }
    int ratingOffset = 0;
    
    if (count[0] > 10 && (count[5] + count[4])  == 0) ratingOffset = 1;
    if (count[0] > 10 && (count[1] + count[2])  == 0) ratingOffset = -1;
    
    String newKey = key.toString() + "," + ratingOffset;
    output.collect(new Text(newKey), NullWritable.get());
  }
}
