package edu.neu.info7250.review_classification;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbabilityMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Configuration conf = context.getConfiguration();
		// long num_pos_review = conf.getLong("num_pos_review", -1);
		// long num_neg_review = conf.getLong("num_neg_review", -1);
		long num_pos_word = conf.getLong("num_pos_word", -1);
		long num_neg_word = conf.getLong("num_neg_word", -1);
		long num_unique_pos_word = conf.getLong("num_unique_pos_word", -1);
		long num_unique_neg_word = conf.getLong("num_unique_neg_word", -1);

		if (num_pos_word < 0 || num_neg_word < 0 || num_unique_pos_word < 0 || num_unique_neg_word < 0)
			return;
		try {
			String[] arr = line.split(",");
			if (arr[1].startsWith("pos_count")) {
				double pos_pro = 0;
				double neg_pro = 0;
				int pos_count = Integer.parseInt(arr[1].split(":")[1]);
				int neg_count = Integer.parseInt(arr[2].split(":")[1]);
				pos_pro = (pos_count + 1.0) / (num_pos_word + num_unique_pos_word);
				neg_pro = (neg_count + 1.0) / (num_neg_word + num_unique_neg_word);
//				System.out.println("pos_pro" + pos_pro + "-------" + "neg_pro");
				// context.write(new Text(arr[0]), new Text("pos_pro" +
				// pos_pro));
				// context.write(new Text(arr[0]), new Text("neg_pro" +
				// neg_pro));
				context.write(new CompositeKeyWritable(arr[0], new StringBuilder().append("pos_pro:").append(pos_pro)
						.append("@@@@@@").append("neg_pro:").append(neg_pro).toString()), NullWritable.get());
			} else {
				context.write(new CompositeKeyWritable(arr[0], arr[1]), NullWritable.get());
			}
		} catch (Exception e) {
			System.out.println("----------------something wrong-------------------------");
			e.printStackTrace();
		}
	}
}
