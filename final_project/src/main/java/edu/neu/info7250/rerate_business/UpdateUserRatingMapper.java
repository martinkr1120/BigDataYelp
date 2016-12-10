package edu.neu.info7250.rerate_business;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;
import java.lang.StringBuilder;

public class UpdateUserRatingMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    String[] v = value.toString().trim().split(",");
    
    if (v.length == 2){
      output.collect(new Text(v[0]), new Text("---offset:" + v[1]));
    }
    else if (v.length == 4){
      output.collect(new Text(v[0]), new Text(v[1] + "," + v[2] + "," + v[3]));
    }
  }
}

