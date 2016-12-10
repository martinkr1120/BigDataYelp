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

public class CleanNonactiveUserReviewReducer extends MapReduceBase
    implements
      Reducer<Text, Text, Text, NullWritable> {

  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, NullWritable> output,
      Reporter reporter) throws IOException {

    reporter.setStatus(key.toString());
    
    /*
     * This is a little trick.
     * Because we know that each nonactive user will not have more than 5 reviews.
     * so include the mark, we will have at most 5 values in iterator. 
     * 
     * Can't find a better way to do this.
     */
    boolean activeUser = true;
    Stack<String> stack = new Stack<String>();
    int i = 0;
    while (values.hasNext()) {
      String v = values.next().toString().trim();
      if (v.equals("---FP7250_nonActive")){
        activeUser = false;
        break;
      }
      else if(v.equals("---FP7250_Active")){
        activeUser = true;
        break;
      }
      else{
        stack.push(v);
      }
      i++;
    }
    System.out.println(i);
    //if the user is nonactive, will not output anything.
    if (!activeUser) return;
    while (!stack.isEmpty()){
      String v = stack.pop();
      String newValue = key.toString() + "," + v;
      output.collect(new Text(newValue), NullWritable.get());
    }
    while (values.hasNext()) {
      String v = values.next().toString().trim();
      String newValue = key.toString() + "," + v;
      output.collect(new Text(newValue), NullWritable.get());
    }
  }
}
