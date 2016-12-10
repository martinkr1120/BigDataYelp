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

public class UserRatingHabitMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, IntWritable> {

  JSONParser parser = new JSONParser();
  
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
      Reporter reporter) throws IOException {
    
    String[] v = value.toString().trim().split(",");
   if (v.length == 4){
      output.collect(new Text(v[0]), new IntWritable(Integer.parseInt(v[2])));
    }
  }
}

