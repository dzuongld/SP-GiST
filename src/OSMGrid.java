import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.*;
public class OSMGrid extends Configured implements Tool {
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
				int c = ':';
				StringBuilder temp = new StringBuilder();
				for(; start < value.getLength(); start++){
					c = value.charAt(start);
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
					//My Job isn't to worry about java not having unsigned values because they are stupid
					id.set(404);
				}
				
				start = value.find(" lat", start);
				if(start == -1)
					break;
				start += 6;
				c = ':';
				temp.delete(0, temp.length());
				for(; start < value.getLength(); start++){
					c = value.charAt(start);
					if(c!='"')
						temp.append((char)c);
					else
						break;
				}
				if(start > value.getLength())
					break;
				node.setY(Double.parseDouble(temp.toString()));
				if(node.getY() < minlat || node.getY() > maxlat){
					continue; //Don't care about nodes outside our area of interest
				}
				
				start = value.find(" lon", start);
				if(start == -1)
					break;
				start += 6;
				c = ':';
				temp.delete(0, temp.length());
				for(; start < value.getLength(); start++){
					c = value.charAt(start);
					if(c!='"')
						temp.append((char)c);
					else
						break;
				}
				if(start > value.getLength())
					break;
				node.setX(Double.parseDouble(temp.toString()));
				if(node.getX() < minlon || node.getX() > maxlon)
					continue; //Don't care about nodes outside our area of interest
				context.write(node, id);
			}
				
		}
	}

	public static class GridPartitioner extends Partitioner<WritablePoint, LongWritable> implements Configurable{
		private double minlat;
		private double maxlat;
		private double minlon;
		private double maxlon;
		private Configuration conf;
		public void setConf(Configuration conf){
			this.conf = conf;
		}
		public Configuration getConf(){
			return conf;
		}
		public int getPartition(WritablePoint key, LongWritable value, int numReduceTasks){
			int grid = (int) Math.sqrt(numReduceTasks);
			minlat = conf.getDouble(OSMMapper.MINLAT,0);
			minlon = conf.getDouble(OSMMapper.MINLON,0);
			maxlat = conf.getDouble(OSMMapper.MAXLAT,0);
			maxlon = conf.getDouble(OSMMapper.MAXLON,0);
			double w = (maxlon - minlon)/grid;
			double h = (maxlat - minlat)/grid;
			int ret = -1;
			for(int i = 0; i < grid; i++){
				double comp = minlat + (double)(i+1) * h;
				if(key.getY() < comp){
					ret = i * grid; //determine highside value
					break;
				}
			}
			if(ret == -1){
				//We had a slight double imprecision problem assume we didn't properly arrive at maxlat
				ret = grid*grid - grid;
			}
			for(int i = 0; i < grid; i++){
				if(key.getX() < minlon + (double)(i+1) * w){
					ret = ret+i;
					return ret; //determine lowside value
				}
			}
			ret = ret + grid-1;
			return ret;
		}
	}

	public static class OSMReducer extends Reducer<WritablePoint, LongWritable, WritablePoint, Text>{
		private MultipleOutputs out;
		private int part;
		private int grid;
		public static String GRID = "mapreduce.reducer.grid";
		public void setup(Context context){
			out = new MultipleOutputs(context);
			Configuration conf = context.getConfiguration();
			part = conf.getInt("mapred.task.partition", 0);
			grid = conf.getInt(GRID, 0);
		}
		public void reduce(WritablePoint key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			String ids = new String("");
			for(LongWritable val : values){
				ids += val.toString() + ", ";
			}
			Text text = new Text(ids);
			int x = part/grid;
			int y = part % grid;
			out.write(key, text,x+"/"+y);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			out.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 7){
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "OSM-Gridding");
		job.setJarByClass(OSMGrid.class);
		job.setOutputKeyClass(WritablePoint.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(OSMMapper.class);
		job.setPartitionerClass(GridPartitioner.class);
		job.setReducerClass(OSMReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().set(OSMMapper.MINLAT, args[2]);
		job.getConfiguration().set(OSMMapper.MINLON, args[3]);		
		job.getConfiguration().set(OSMMapper.MAXLAT, args[4]);
		job.getConfiguration().set(OSMMapper.MAXLON, args[5]);
		job.getConfiguration().set(OSMReducer.GRID, args[6]);
		job.setNumReduceTasks(Integer.parseInt(args[6])*Integer.parseInt(args[6]));
		boolean succ = job.waitForCompletion(true);
		return succ ? 0: 1;
	}
	//<bounds minlat="37.62705" minlon="-88.10327" maxlat="41.76301" maxlon="-84.50399"/>
	public static void main(String[] args) throws Exception {
		OSMGrid grid = new OSMGrid();
		System.exit(ToolRunner.run(grid, args));
	}

public class WritableRectangle implements WritableComparable<WritableRectangle>{
		private double x;		
		private double y;
		private double h;
		private double w;
		public WritableRectangle() {}

		public WritableRectangle(double x, double y, double h, double w){
			this.y = y;
			this.h = h;
			this.x = x;
			this.w = w;
			if(h < 0){
				this.y = this.y + h;
				this.h = -h;
			}
			if(w < 0){
				this.x = this.x + w;
				this.w = -w;
			}
		}
		public void readFields(DataInput in) throws IOException {
			x = in.readDouble();
			y = in.readDouble();
			h = in.readDouble();
			w = in.readDouble();
		}
		public void write(DataOutput out) throws IOException {
			out.writeDouble(x);
			out.writeDouble(y);
			out.writeDouble(h);
			out.writeDouble(w);
		}
		public int hashCode() {
			//I don't really care about a high collision rate
			return (int) (x*y-h+w); 
		}
		public boolean equals(Object o){
			if(!(o instanceof WritableRectangle))
				return false;
			WritableRectangle other = (WritableRectangle) o;
			return this.x == other.x && this.y == other.y && this.h == other.h && this.w == other.w;
		}
		public int compareTo(WritableRectangle o){
			double x1 = this.x;
			double x2 = o.x;
			return (x1<x2 ? -1 : (x1==x2 ? 0 : 1));
		}
		public String toString() {
			return Double.toString(x) + Double.toString(y) + Double.toString(h) + Double.toString(w);
		}
	}

	public static class WritablePoint implements WritableComparable<WritablePoint>{
		private double x;
		private double y;
		public WritablePoint() {}

		public WritablePoint(double x, double y){
			this.y = y;
			this.x = x;
		}
		public void setX(double x){
			this.x = x;
		}
		public double getX(){
			return x;
		}
		public void setY(double y){
			this.y = y;
		}
		public double getY(){
			return y;
		}
		public void readFields(DataInput in) throws IOException {
			x = in.readDouble();
			y = in.readDouble();
		}
		public void write(DataOutput out) throws IOException {
			out.writeDouble(x);
			out.writeDouble(y);
		}
		public int hashCode() {
			//I don't really care about a high collision rate
			return (int) (x*y); 
		}
		public boolean equals(Object o) {
			if(!(o instanceof WritablePoint))
				return false;
			WritablePoint other = (WritablePoint)o;
			return this.x == other.x && this.y == other.y;
		}
		public int compareTo(WritablePoint o){
			double x1 = this.x;
			double x2 = o.x;
			return (x1<x2 ? -1 : (x1==x2 ? 0 : 1));
		}
		public String toString() {
			return "Lon: " + Double.toString(x) + " Lat: "+ Double.toString(y);
		}
	}

	
}