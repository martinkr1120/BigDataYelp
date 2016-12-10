package edu.neu.info7250.rerate_business;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.lang.StringBuilder;

public class UpdateUserRatingReducer extends MapReduceBase
    implements
      Reducer<Text, Text, Text, NullWritable> {

  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, NullWritable> output,
      Reporter reporter) throws IOException {

    reporter.setStatus(key.toString());
    
    int offset = 0;
    Stack<String> stack = new Stack<String>();
    
    while (values.hasNext()) {
      String v = values.next().toString().trim();
      //I used this trick to let stack as small as possible.
      //But it is still not good way to deal with bigdata.
      if (v.startsWith("---offset:")){
        offset = Integer.parseInt(v.split(":")[1]);
        break;
      }
      else{
        stack.push(v);
      }
    }
    while (!stack.isEmpty()){
      String[] tmp = stack.pop().split(",");
      int score = Integer.parseInt(tmp[1]) + offset;
      String newKey = tmp[0] + "," + score + "," + tmp[2];
      output.collect(new Text(newKey), NullWritable.get());
    }
    while (values.hasNext()) {
      String[] tmp = values.next().toString().trim().split(",");
      int score = Integer.parseInt(tmp[1]) + offset;
      String newKey = tmp[0] + "," + score + "," + tmp[2];
      output.collect(new Text(newKey), NullWritable.get());
    }
  }
}
