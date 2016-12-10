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

public class NonActiveUserMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, Text> {

  JSONParser parser = new JSONParser();
  
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    JSONObject obj;
    long review_count = 0;
    long fans = 0;
    long elite = 0;
    long friends = 0;
    long votes = 0;
    long compliments = 0;
    String user_id = "";

    try {
      obj = (JSONObject) parser.parse(value.toString());
      
      user_id  = (String) obj.get("user_id");
      
      review_count = (Long) obj.get("review_count");
      
      fans = (Long) obj.get("fans");
      
      JSONArray eliteArray = (JSONArray) obj.get("elite");
      elite = eliteArray.size();
      
      JSONArray friendsArray = (JSONArray) obj.get("friends");
      friends = friendsArray.size();
      
      JSONObject objVotes = (JSONObject) obj.get("votes");
      
      votes = (Long) objVotes.get("funny") + (Long) objVotes.get("useful") + (Long) objVotes.get("cool");
      
      JSONObject objCompliments = (JSONObject) obj.get("compliments");
      compliments = objCompliments.size();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    if ((review_count < 5 && fans == 0 && elite == 0 && friends == 0 && votes == 0 && compliments == 0)
        && user_id != "") {
      output.collect(new Text(user_id), new Text("---FP7250_nonActive"));
    }
    else{
      output.collect(new Text(user_id), new Text("---FP7250_Active"));
    }
  }
}

