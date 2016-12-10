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
import java.text.DecimalFormat;
import java.util.*;

public class RerateBusinessReducer extends MapReduceBase
    implements
      Reducer<Text, Text, Text, NullWritable> {
  
  DecimalFormat decimalFormat = new DecimalFormat("0.0");
  
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, NullWritable> output,
      Reporter reporter) throws IOException {

    reporter.setStatus(key.toString());
    int[] count = new int[13];
    double[] scoreSum = new double[13];
    
    while (values.hasNext()) {
      String[] v = values.next().toString().split(",");
      int score = Integer.parseInt(v[0].trim());
      int month = Integer.parseInt(v[1].trim());
      count[month] += 1;
      count[0] += 1;
      scoreSum[month] += score;
      scoreSum[0] += score;
    }
    
//    String newKey = key.toString() + ",{";
//    
//    for (int i = 0; i < 13; i++){
//      String newScore = "0";
//      if (count[i] > 0){
//        newScore = decimalFormat.format(scoreSum[i] / count[i]);
//      }
//      if (i>0) newKey += ", ";
//      newKey += "\"" + i +"\": {\"score\": " + newScore + ", \"count\": " + count[i] + "}";
//    }
//    
//    newKey += "}";
    
    String newKey = key.toString();
    
    for (int i = 0; i < 13; i++){
      String newScore = "0";
      if (count[i] > 0){
        newScore = decimalFormat.format(scoreSum[i] / count[i]);
      }
      newKey += "," + newScore + "," + count[i];
    }
    output.collect( new Text(newKey), NullWritable.get());
  }
}
