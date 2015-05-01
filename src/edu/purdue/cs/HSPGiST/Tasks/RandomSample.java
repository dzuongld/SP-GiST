package edu.purdue.cs.HSPGiST.Tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.AbstractClasses.Predicate;
import edu.purdue.cs.HSPGiST.HadoopClasses.LocalHSPGiSTOutputFormat;
import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

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
	
	Parser<MKIn,MVIn,MKOut,MVOut> parse;
	HSPIndex<MKOut,MVOut> index;

	public RandomSample(Parser<MKIn,MVIn,MKOut,MVOut> parser, HSPIndex<MKOut,MVOut> index) {
		super();
		parse = parser;
		this.index = index;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length == 5)
			conf.set("mapreduce.mapper.sample-percentage", args[4]);
		conf.set("mapreduce.mapper.parserClass", parse.getClass().getName());
		conf.set("mapreduce.reducer.indexClass", index.getClass().getName());
		Job job = Job.getInstance(conf, "Random_Sample");
		job.setJarByClass(RandomSample.class);
		long defaultBlockSize = 0;
		// Get the number of mappers that will be launched for this task
		// So we can determine the number of reducers
		int numOfReduce = 1;
		long inputFileLength = 0;
		FileSystem fileSystem = FileSystem.get(this.getConf());
		inputFileLength = fileSystem.getContentSummary(new Path(args[3]))
				.getLength();
		defaultBlockSize = fileSystem.getDefaultBlockSize(new Path(args[3]));
		if (inputFileLength > 0 && defaultBlockSize > 0) {
			numOfReduce = (int) (((inputFileLength / defaultBlockSize) + 1) * CommandInterpreter.REDUCERPERSPLIT);
		}
		// set numOfReduce to the first valid number lte a cuurent number
		// E.g. Quadtree can only support partitions of size 1+3n for n >= 0
		// So if numOfReduce == 3 we must increase that to 4
		if ((numOfReduce - 1) % (index.numSpaceParts - 1) != 0)
			numOfReduce = ((numOfReduce - 1) / (index.numSpaceParts - 1) + 1)
					* (index.numSpaceParts - 1) + 1;
		job.getConfiguration().setInt("mapreduce.reducer.numReduce", numOfReduce);
		job.setMapOutputKeyClass(parse.keyout);
		job.setMapOutputValueClass(parse.valout);
		job.setOutputKeyClass(HSPIndexNode.class);
		job.setOutputValueClass(HSPReferenceNode.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(LocalHSPGiSTOutputFormat.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(SampleMap.class);
		job.setReducerClass(GlobalReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[3]));
		
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.GLOBALCONSTRUCT);
		sb.append(CommandInterpreter.postScript);
		Path globalIndexFile = new Path(sb.toString());
		FileOutputFormat.setOutputPath(job, globalIndexFile);
		boolean succ = job.waitForCompletion(true);
		
		if(!succ){
			FileSystem hdfs = FileSystem.get(job.getConfiguration());
			hdfs.delete(globalIndexFile, true);
		}
		return succ ? 0 : 1;
	}

	

	public static class SampleMap<MKIn, MVIn, MKOut, MVOut> extends
			Mapper<MKIn, MVIn, MKOut, MVOut> {
		private Random rand;
		private double percent;
		private Parser<MKIn,MVIn,MKOut,MVOut> parse;

		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				parse = (Parser<MKIn,MVIn,MKOut,MVOut>) Class.forName(conf.get("mapreduce.mapper.parserClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
			}
			rand = new Random();
			percent = context.getConfiguration().getDouble(
					"mapreduce.mapper.sample-percentage", .0001);
			if (percent > 1) {
				percent = percent / 100;
			}
		}

		public void map(MKIn key, MVIn value, Context context)
				throws IOException, InterruptedException {
			if (parse.isArrayParse) {
				ArrayList<Pair<MKOut, MVOut>> list = parse.arrayParse(key,
						value);
				Pair<MKOut, MVOut> pair = null;
				for (int i = 0; i < list.size(); i++) {
					pair = list.get(i);
					if (rand.nextDouble() < percent)
						context.write(pair.getFirst(), pair.getSecond());
				}
			} else {
				Pair<MKOut, MVOut> pair = parse.parse(key, value);
				if (rand.nextDouble() < percent)
					context.write(pair.getFirst(), pair.getSecond());
			}
		}
	}

	public static class GlobalReducer<MKOut, MVOut> extends
			Reducer<MKOut, MVOut, HSPIndexNode<MKOut,MVOut>, HSPReferenceNode<MKOut,MVOut>> {
		HSPIndex<MKOut, MVOut> index;
		int numOfReducers = 1;

		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			try {
				index = (HSPIndex<MKOut,MVOut>) Class.forName(
						context.getConfiguration().get("mapreduce.reducer.indexClass"))
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
			}
			numOfReducers = context.getConfiguration().getInt("mapreduce.reducer.numReduce", 1);
		}

		@SuppressWarnings("unchecked")
		public void reduce(MKOut key, Iterable<MVOut> values, Context context) {
			index.samples.add(((Copyable<MKOut>)key).copy());
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			index.setupPartitions(numOfReducers);
			HSPNode<MKOut,MVOut> nodule = index.globalRoot;
			index.globalRoot.getSize();
			ArrayList<HSPNode<MKOut,MVOut>> stack = new ArrayList<HSPNode<MKOut,MVOut>>();
			//Write in preorder to file
			while (!(stack.size() == 0 && nodule == null)) {
				if (nodule != null) {
					if (nodule instanceof HSPIndexNode<?, ?>) {
						HSPIndexNode<MKOut,MVOut> temp = (HSPIndexNode<MKOut,MVOut>) nodule;
						context.write( temp, null);
						for (int i = 0; i < temp.getChildren().size(); i++) {
							stack.add((HSPNode<MKOut,MVOut>) temp.getChildren().get(i));
						}
						nodule = stack.remove(stack.size() - 1);
					} else {
						context.write(null, (HSPReferenceNode<MKOut,MVOut>) nodule);
						nodule = null;
					}
				} else
					nodule = stack.remove(stack.size() - 1);
			}
			//Write partPreds to file for quick access for reducers during local construction
			FileSystem hdfs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream output = hdfs.create(new Path("localRoots/partRoots"));
			output.writeInt(index.partRoots.size());
			for (Pair<Predicate, IntWritable> sample : ((ArrayList<Pair<Predicate, IntWritable>>) index.partRoots)) {
				((WritableComparable<Pair<Predicate, IntWritable>>) sample)
						.write(output);
			}
			output.close();
			
		}
	}

}