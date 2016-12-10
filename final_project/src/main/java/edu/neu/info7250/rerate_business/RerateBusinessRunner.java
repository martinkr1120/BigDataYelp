package edu.neu.info7250.rerate_business;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class RerateBusinessRunner {

  public static void main(String[] args) throws Exception {
    JobConf countReviewTipMapperConf = new JobConf(RerateBusinessRunner.class);
    
    Path inPath = new Path("output","revaluedReview");
    Path outPath = new Path("output","ratedBusiness");
    
    try {
      /*
       * Graph Builder JobConf
       */
      countReviewTipMapperConf.setJobName("Rerate Business");
      countReviewTipMapperConf.setMapperClass(RerateBusinessMapper.class);
      countReviewTipMapperConf.setMapOutputValueClass(Text.class);
      countReviewTipMapperConf.setReducerClass(RerateBusinessReducer.class);
      countReviewTipMapperConf.setOutputKeyClass(Text.class);
      countReviewTipMapperConf.setOutputValueClass(NullWritable.class);
      FileInputFormat.setInputPaths(countReviewTipMapperConf, inPath);
      FileOutputFormat.setOutputPath(countReviewTipMapperConf, outPath);
      
      FileSystem dfs = FileSystem.get(outPath.toUri(), countReviewTipMapperConf);
      if (dfs.exists(outPath)) {
        dfs.delete(outPath, true);
      }
      
      JobClient.runJob(countReviewTipMapperConf);
    } 
    catch (Exception e) {
      e.printStackTrace();
    } 
  }
}
