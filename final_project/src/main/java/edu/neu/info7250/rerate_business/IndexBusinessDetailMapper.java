package edu.neu.info7250.rerate_business;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;
import java.lang.StringBuilder;

public class IndexBusinessDetailMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, Text> {

  JSONParser parser = new JSONParser();
  
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    JSONObject obj;
    String business_id = "";
    try {
      obj = (JSONObject) parser.parse(value.toString());
      business_id =  (String) obj.get("business_id");
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (business_id != ""){
      output.collect(new Text(business_id), value);
    }
  }
}

