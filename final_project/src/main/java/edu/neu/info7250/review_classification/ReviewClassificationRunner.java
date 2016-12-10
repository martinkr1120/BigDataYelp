package edu.neu.info7250.review_classification;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReviewClassificationRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = null;

//		conf.set("mapred.child.java.opts", "-Xmx7168m");
		FileSystem hdfs = FileSystem.get(conf);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		System.out.println("---------------" + Arrays.toString(otherArgs) + "-----------");
		if (otherArgs.length != 3) {
			System.out.println(Arrays.toString(otherArgs));
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		String input = otherArgs[1];
		String outputBase = otherArgs[2];
		Path inputPath = new Path(input, "yelp_academic_dataset_review.json");
		Path original_input = inputPath;
		Path outputPath = new Path(outputBase, "review_word_count");



		job = Job.getInstance(conf, "Count_Pos_Neg_Total");

		job.setJarByClass(ReviewClassificationRunner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(CountMapper.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
		Counter counter_pos_review = job.getCounters().findCounter("Counter_Group", "num_pos_review");
		Counter counter_neg_review = job.getCounters().findCounter("Counter_Group", "num_neg_review");
		Counter counter_pos_word = job.getCounters().findCounter("Counter_Group", "num_pos_word");
		Counter counter_neg_word = job.getCounters().findCounter("Counter_Group", "num_neg_word");
		Counter counter_unique_pos_word = job.getCounters().findCounter("Counter_Group", "num_unique_pos_word");
		Counter counter_unique_neg_word = job.getCounters().findCounter("Counter_Group", "num_unique_neg_word");

		long num_pos_review = counter_pos_review.getValue();
		long num_neg_review = counter_neg_review.getValue();
		long num_pos_word = counter_pos_word.getValue();
		long num_neg_word = counter_neg_word.getValue();
		long num_unique_pos_word = counter_unique_pos_word.getValue();
		long num_unique_neg_word = counter_unique_neg_word.getValue();
		long num_review = num_pos_review + num_neg_review;

		System.out.println(num_review);
		System.out.println(num_pos_review);
		System.out.println(num_neg_review);
		System.out.println(num_pos_word);
		System.out.println(num_neg_word);
		System.out.println(num_unique_pos_word);
		System.out.println(num_unique_neg_word);



		 // debug

//		 long num_pos_review = 1774673;
//		 long num_neg_review = 450540;
//		 long num_pos_word = 154127611;
//		 long num_neg_word = 51339607;
//		 long num_unique_pos_word = 328919;
//		 long num_unique_neg_word = 160926;
//		 long num_review = num_pos_review + num_neg_review;
		


		/**
		 * Job 2
		 * 
		 * 
		 */
		inputPath = new Path(outputBase, "review_word_count");
		outputPath = new Path(outputBase, "review_probability");


		
		conf.setLong("num_review", num_review);
		conf.setLong("num_pos_review", num_pos_review);
		conf.setLong("num_neg_review", num_neg_review);
		
		float pos_pro_review = (float) ((1.0 * num_pos_review) / num_review);
		float neg_pro_review = (float) ((1.0 * num_neg_review) / num_review);
		conf.setFloat("pos_pro_review", pos_pro_review);
		conf.setFloat("neg_pro_review", neg_pro_review);
		conf.setLong("num_pos_word", num_pos_word);
		conf.setLong("num_neg_word", num_neg_word);
		conf.setLong("num_unique_pos_word", num_unique_pos_word);
		conf.setLong("num_unique_neg_word", num_unique_neg_word);
		
		
		job = Job.getInstance(conf, "Probabilities_review");
		job.setJarByClass(ReviewClassificationRunner.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setMapperClass(ProbabilityMapper.class);

		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setSortComparatorClass(SecondarySortCompKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		
		job.setNumReduceTasks(1);
		job.setReducerClass(ProbabilityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
		
		
		/**
		 *  Job 3
		 */
		
		inputPath = new Path(outputBase, "review_probability");
		outputPath = new Path(outputBase, "review_classification");
			
		
		job = Job.getInstance(conf, "Classification_review");
		job.setJarByClass(ReviewClassificationRunner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(ReviewProbabilityMapper.class);
		
		job.setNumReduceTasks(1);
		job.setReducerClass(ReviewProbabilityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
		
		
		

		inputPath = new Path(outputBase, "review_classification");
		outputPath = new Path(outputBase, "review_results");
		
		job = Job.getInstance(conf, "Final_Result_Job");
		job.setJarByClass(ReviewClassificationRunner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(DecisionMapper.class);

		job.setNumReduceTasks(1);
		job.setReducerClass(DecisionReducer.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		// job.setOutputFormatClass(NullOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "positive", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "negative", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileInputFormat.addInputPath(job, original_input);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);

		Counter result_counter = job.getCounters().findCounter("Counter_Group", "result_positive");
		System.out.println("positive_result: " + result_counter.getValue());
		result_counter = job.getCounters().findCounter("Counter_Group", "result_negative");
		System.out.println("negative_result: " + result_counter.getValue());

	}

}
