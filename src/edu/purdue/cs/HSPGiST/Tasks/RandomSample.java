package edu.purdue.cs.HSPGiST.Tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;

/**
 * A simple random sampler
 * 
 * @author Stefan Brinton
 *
 * @param <MKIn>
 * @param <MVIn>
 * @param <MKOut>
 * @param <MVOut>
 */
public class RandomSample<MKIn, MVIn, MKOut, MVOut> extends Configured
		implements Tool {
	@SuppressWarnings("rawtypes")
	static Parser parse;
	@SuppressWarnings("rawtypes")
	static HSPIndex index;

	@SuppressWarnings({ "rawtypes" })
	public RandomSample(Parser parser, HSPIndex index) {
		super();
		parse = parser;
		RandomSample.index = index;
	}

	public static class Map<MKIn, MVIn, MKOut, MVOut> extends
			Mapper<MKIn, MVIn, MKOut, MVOut> {
		private Random rand;
		private double percent;
		public void setup(Context context) {
			rand = new Random();
			percent = context.getConfiguration().getDouble("mapreduce.mapper.sample-percentage", .01);
			if(percent > 1){
				percent = percent/100;
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void map(MKIn key, MVIn value, Context context)
				throws IOException, InterruptedException {
			if (parse.isArrayParse) {
				ArrayList<Pair<MKOut, MVOut>> list = parse.arrayParse(key,
						value);
				Pair<MKOut, MVOut> pair = null;
				for (int i = 0; i < list.size(); i++) {
					pair = list.get(i);
					if (rand.nextDouble() < percent)
						index.samples.add(((Copyable) pair.getFirst()).copy());
				}
			} else {
				Pair<MKOut, MVOut> pair = parse.parse(key, value);
				if (rand.nextDouble() < percent)
					index.samples.add(((Copyable) pair.getFirst()).copy());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.mapper.sample-percentage", args[4]);
		Job job = Job.getInstance(conf, "Random_Sample");
		job.setJarByClass(RandomSample.class);

		job.setOutputKeyClass(parse.keyout);
		job.setOutputValueClass(parse.valout);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(Map.class);
		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path("temp"));

		boolean succ = job.waitForCompletion(true);
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path("temp"), true);
		return succ ? 0 : 1;
	}

}