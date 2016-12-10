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


public class SearchIndexForBusinessRunner {

  public static void main(String[] args) throws Exception {
    
    JobConf countReviewTipMapperConf = new JobConf(SearchIndexForBusinessRunner.class);
    
    Path inPath = new Path("input","yelp_academic_dataset_business.json");
    Path reviewPath = new Path("output","ratedBusiness");
    Path outPath = new Path("output","searchIndexForBusiness");
    
    try {
      /*
       * Graph Builder JobConf
       */
      countReviewTipMapperConf.setJobName("build search index for business");
      countReviewTipMapperConf.setMapperClass(SearchIndexForBusinessMapper.class);
      countReviewTipMapperConf.setMapOutputValueClass(Text.class);
      countReviewTipMapperConf.setReducerClass(SearchIndexForBusinessReducer.class);
      countReviewTipMapperConf.setOutputKeyClass(Text.class);
      countReviewTipMapperConf.setOutputValueClass(NullWritable.class);
      FileInputFormat.setInputPaths(countReviewTipMapperConf, inPath);
      FileInputFormat.addInputPath(countReviewTipMapperConf, reviewPath);
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
