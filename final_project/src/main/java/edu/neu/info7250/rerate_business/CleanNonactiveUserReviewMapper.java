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

public class CleanNonactiveUserReviewMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, Text> {

  JSONParser parser = new JSONParser();
  
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    String valueString = value.toString();
    
    if (valueString.indexOf("---FP7250_nonActive") != -1 ||
        valueString.indexOf("---FP7250_Active") != -1){
      String[] tmp = valueString.split("\t");
      output.collect(new Text(tmp[0]), new Text(tmp[1]));
    }
    else{
      JSONObject obj;
      String business_id = "";
      String user_id = "";
      String date = "";
      int month = 0;
      long stars = 0;
      try {
        obj = (JSONObject) parser.parse(value.toString());
        user_id  = (String) obj.get("user_id");
        business_id = (String) obj.get("business_id");
        date = (String) obj.get("date");
        stars  = (Long) obj.get("stars");
        month = Integer.parseInt(date.substring(5, 7));
      } catch (Exception e) {
        e.printStackTrace();
      }
  
      if (user_id != "" && business_id != "" && month > 0 && month < 13 && stars > 0 && stars< 6){
        output.collect(new Text(user_id), new Text(business_id + "," + stars + "," + month));
      }
    }
  }
}

