import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.purdue.cs.HSPGiST.SupportClasses.CopyWritableLong;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.SupportClasses.WritablePoint;


public class LinearOSM extends Configured implements Tool {
	public static class OSMMapper extends Mapper<Object, Text, WritablePoint, LongWritable> {
		private WritablePoint node = new WritablePoint(0,0);
		private LongWritable id = new LongWritable(0);
		private double minlat;
		private double maxlat;
		private double minlon;
		private double maxlon;
		public static String MINLAT = "mapreduce.mapper.minlat";
		public static String MAXLAT = "mapreduce.mapper.maxlat";
		public static String MINLON = "mapreduce.mapper.minlon";
		public static String MAXLON = "mapreduce.mapper.maxlon";
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			minlat = conf.getDouble(MINLAT, 0);
			minlon = conf.getDouble(MINLON, 0);
			maxlat = conf.getDouble(MAXLAT, 0);
			maxlon = conf.getDouble(MAXLON, 0);
			
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			int start = 0;
			while(start < value.getLength()){
				//Find the start of a node
				start = value.find("node", start);
				
				if(start == -1)
					break;

				//Find the id of that node
				start = value.find(" id",start);
				if(start == -1)
					break;
				start += 5;
				char c = ':';
				StringBuilder temp = new StringBuilder();
				for(; start < value.getLength(); start++){
					c = (char) value.charAt(start);
					if(c!='"')
						temp.append(c);
					else
						break;
				}
				if(start > value.getLength())
					break;
				try{
					id.set(Long.parseLong(temp.toString()));
				}
				catch(NumberFormatException e){
					//Java doesn't have unsigned ints and I'm not especially concerned with ids as they aren't especially important for debugging
					id.set(404);
				}
				
				start = value.find(" lat", start);
				if(start == -1)
					break;
				start += 6;
				c = ':';
				temp.delete(0, temp.length());
				for(; start < value.getLength(); start++){
					c = (char) value.charAt(start);
					if(c!='"')
						temp.append((char)c);
					else
						break;
				}
				if(start > value.getLength())
					break;
				node.setY(Double.parseDouble(temp.toString()));
				start = value.find(" lon", start);
				if(start == -1)
					break;
				start += 6;
				c = ':';
				temp.delete(0, temp.length());
				for(; start < value.getLength(); start++){
					c = (char) value.charAt(start);
					if(c!='"')
						temp.append((char)c);
					else
						break;
				}
				if(start > value.getLength())
					break;
				node.setX(Double.parseDouble(temp.toString()));
				if(minlon <= node.getX() && node.getX() <= maxlon && minlat <= node.getY() && node.getY() <= maxlat)
					context.write(node, id);
			}
		}
	}

	public static class OSMReducer extends Reducer<WritablePoint, LongWritable, WritablePoint, LongWritable>{
		public void reduce(WritablePoint key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			for(LongWritable val : values)
				context.write(key, val);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(OSMMapper.MINLON, args[1]);
		conf.set(OSMMapper.MINLAT, args[2]);
		conf.set(OSMMapper.MAXLON, args[3]);
		conf.set(OSMMapper.MAXLAT, args[4]);
		Job job = Job.getInstance(conf, "OSM-Linear");
		job.setJarByClass(OSMGrid.class);
		job.setOutputKeyClass(WritablePoint.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(OSMMapper.class);
		job.setReducerClass(OSMReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("Validation"));
		if(args.length == 6)
			job.setNumReduceTasks(1);
		else
			job.setNumReduceTasks(0);
		boolean succ = job.waitForCompletion(true);
		return succ ? 0: 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinearOSM(), args));
	}
	
}