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

public class SearchIndexForBusinessReducer extends MapReduceBase
    implements
      Reducer<Text, Text, Text, NullWritable> {

  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, NullWritable> output,
      Reporter reporter) throws IOException {

    reporter.setStatus(key.toString());
    
    String[] newValue = new String[2];
    
    System.out.println(key.toString());
    
    while (values.hasNext()) {
      String v = values.next().toString().trim();
      
      //I used this trick to let stack as small as possible.
      //But it is still not good way to deal with bigdata.
      if (v.startsWith("l:")){
        newValue[0] = v.substring(2);
        System.out.println(v);
      }
      else if(v.startsWith("r:")){
        newValue[1] = v.substring(2);
      }
    }
    
    if (newValue[0] != null && newValue[0] != "" && newValue[1] != null && newValue[1] != ""){
      output.collect(new Text(key.toString() + "," + newValue[0] + "," + newValue[1]), NullWritable.get());
    }
  }
}
