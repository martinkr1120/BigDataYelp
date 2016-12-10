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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;
import java.lang.StringBuilder;

public class SearchIndexForBusinessMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, Text> {
  
  JSONParser parser = new JSONParser();
  
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    String v = value.toString().trim();
    
    if (v.charAt(0) == '{'){
      JSONObject obj;
      String business_id = "";
      double latitude = 181; //-90 to 90
      double longitude = 181;  //-180 to 180
      try {
        obj = (JSONObject) parser.parse(v);
        business_id =  (String) obj.get("business_id");
        latitude  = (Double) obj.get("latitude");
        longitude = (Double) obj.get("longitude");
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (business_id != "" && latitude != 181 && longitude != 181){
        output.collect(new Text(business_id), new Text("l:" + latitude + "," + longitude));
      }
    }
    else{
      String[] tmp = v.split(",", 2);
      output.collect(new Text(tmp[0]), new Text("r:" + tmp[1]));
    }
  }
}

