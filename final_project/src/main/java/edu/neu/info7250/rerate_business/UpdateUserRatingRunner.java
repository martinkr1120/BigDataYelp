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


public class UpdateUserRatingRunner {

  public static void main(String[] args) throws Exception {
    
    JobConf countReviewTipMapperConf = new JobConf(UpdateUserRatingRunner.class);
    
    Path inPath = new Path("output","ratingHabit");
    Path reviewPath = new Path("output","cleanedReview");
    Path outPath = new Path("output","revaluedReview");
    
    try {
      /*
       * Graph Builder JobConf
       */
      countReviewTipMapperConf.setJobName("Delete reviews from nonactive users and format review");
      countReviewTipMapperConf.setMapperClass(UpdateUserRatingMapper.class);
      countReviewTipMapperConf.setMapOutputValueClass(Text.class);
      countReviewTipMapperConf.setReducerClass(UpdateUserRatingReducer.class);
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
