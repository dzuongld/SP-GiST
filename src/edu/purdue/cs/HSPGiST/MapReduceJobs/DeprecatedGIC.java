package edu.purdue.cs.HSPGiST.MapReduceJobs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

/**
 * This class has been deprecated as setupPartitions now 
 * constructs the global index simultaneously
 * @author stefan
 *
 * @param <T>
 * @param <K>
 * @param <R>
 */
public class DeprecatedGIC<T,K,R> extends Configured implements Tool{
	@SuppressWarnings("rawtypes")
	static Parser parser = null;
	@SuppressWarnings("rawtypes")
	static HSPIndex index;
	@SuppressWarnings("rawtypes")
	public DeprecatedGIC(Parser parser, HSPIndex index){
		super();
		DeprecatedGIC.parser = parser;
		DeprecatedGIC.index = index;
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Global-Construction");
		job.setJarByClass(DeprecatedGIC.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(HSPIndexNode.class);
		job.setOutputValueClass(HSPReferenceNode.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);
		job.setMapperClass(GlobalMap.class);
		job.setReducerClass(GlobalReducer.class);
		FileInputFormat.addInputPath(job, new Path(CommandInterpreter.CONSTRUCTFIRSTOUT + "-" + parser.getClass().getSimpleName() + "-" + index.getClass().getSimpleName() + "-" + args[3]));
		FileOutputFormat.setOutputPath(job, new Path("Preliminary Global"));

		boolean succ = job.waitForCompletion(true);
		if(!succ){
			FileSystem fs = FileSystem.get(getConf());
			fs.delete(new Path(CommandInterpreter.CONSTRUCTSECONDOUT + "-" + parser.getClass().getSimpleName() + "-" + index.getClass().getSimpleName() + "-" + args[3]), true);
		}

		return succ ? 0 : 1;
	}
	
	@SuppressWarnings("rawtypes")
	public static class GlobalMap<T> extends Mapper<HSPIndexNode, HSPLeafNode, Pair<T,IntWritable>, Text> {
		//We only want the first entry from each input file as it contains the root of each local tree
		
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			try{
				context.nextKeyValue();
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
			finally{
				cleanup(context);
			}
		}
		@SuppressWarnings("unchecked")
		public void map(HSPIndexNode key, HSPLeafNode value, Context context) throws IOException, InterruptedException {
			if(key == null){
				for(int i = 0; i < index.partitionPreds.size();i++){
					Pair<T,Integer> pred =  ((Pair<T,Integer>) index.partitionPreds.get(i));
					if(((WritableComparable<T>)value.getPredicate()).equals(pred.getFirst())){
						context.write(new Pair(pred.getFirst(), new IntWritable(pred.getSecond())), new Text(((FileSplit) context.getInputSplit()).getPath().toString()));
					}
				}
			}
			else{
				for(int i = 0; i < index.partitionPreds.size();i++){
					Pair<T,Integer> pred =  ((Pair<T,Integer>) index.partitionPreds.get(i));
					if(((WritableComparable<T>)key.getPredicate()).equals(pred.getFirst())){
						context.write(new Pair(pred.getFirst(), new IntWritable(pred.getSecond())), new Text(((FileSplit) context.getInputSplit()).getPath().toString()));
					}
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class GlobalReducer<T,K,R> extends Reducer<Pair<T,IntWritable>, Text, HSPIndexNode, HSPReferenceNode> {
		ArrayList<Pair<Pair<T,Integer>, Path>> localRoots;
		
		HSPIndexNode<T,K,R> root;
		public void setup(Context con){
			localRoots = new ArrayList<Pair<Pair<T,Integer>,Path>>();
		}
		
		public void reduce(Pair<T,IntWritable> key, Iterable<Text> values, Context context){
			for(Text pathto : values){
				Path path = new Path(pathto.toString());
				if(localRoots.size() == 0){
					localRoots.add(new Pair<Pair<T,Integer>,Path>(new Pair<T,Integer>(key.getFirst(),key.getSecond().get()),path));
					return;
				}
				for(int i = 0 ; i < localRoots.size();i++){
					if(localRoots.get(i).getFirst().getSecond() > key.getSecond().get()){
						localRoots.add(i,new Pair<Pair<T,Integer>,Path>(new Pair<T,Integer>(key.getFirst(),key.getSecond().get()),path));
						return;
					}
				}
				localRoots.add(new Pair<Pair<T,Integer>,Path>(new Pair<T,Integer>(key.getFirst(),key.getSecond().get()),path));
				return;
			}
		}
		
		@SuppressWarnings("unchecked")
		public void cleanup(Context context) throws IOException, InterruptedException{
			
			int low = localRoots.get(0).getFirst().getSecond();
			//root = new HSPIndexNode<T,K,R>(null,null,index,low,1);
			ArrayList<HSPIndexNode<T,K,R>> even = new ArrayList<HSPIndexNode<T,K,R>>();
			ArrayList<HSPIndexNode<T,K,R>> odd = new ArrayList<HSPIndexNode<T,K,R>>();
			odd.add(root);
			int curDep = 2;
			while(localRoots.size() != 0){
				if(even.size() == 0){
					for(int j = 0; j < odd.size(); j++){
						HSPIndexNode temp = odd.get(j);
						for(int i = 0; i< localRoots.size() && localRoots.get(i).getFirst().getSecond() <= low; i++){
							for(int k = 0; k < temp.children.size() && (curDep == low || i == 0);k++){
								if(curDep == low){
									if(((HSPNode) temp.children.get(k)).getPredicate().equals(localRoots.get(i).getFirst().getFirst())){
										temp.children.set(k, new HSPReferenceNode<T,K,R>(temp, (T) ((HSPNode)temp.children.get(k)).getPredicate(), localRoots.get(i).getSecond()));
										localRoots.remove(i--);
										if(localRoots.size()!=0)
											low = localRoots.get(0).getFirst().getSecond();
										break;
									}
									else if(localRoots.size() == 1 || localRoots.get(i+1).getFirst().getSecond() > low){
										if(temp.children.get(k) instanceof HSPLeafNode<?,?,?>){
											//temp.children.set(k, new HSPIndexNode<T,K,R>(temp, (T) ((HSPNode) temp.children.get(k)).getPredicate(),index, low+1, curDep));
										}
										else if(temp.children.get(k) instanceof HSPReferenceNode<?,?,?>)
											continue;
										even.add((HSPIndexNode<T, K, R>) temp.children.get(k));
									}
								}
								else if(i == 0){
									if(temp.children.get(k) instanceof HSPLeafNode<?,?,?>){
										//temp.children.set(k, new HSPIndexNode<T,K,R>(temp, (T) ((HSPNode) temp.children.get(k)).getPredicate(),index, low, curDep));
									}
									else if(temp.children.get(k) instanceof HSPReferenceNode<?,?,?>)
										continue;
									even.add((HSPIndexNode<T, K, R>) temp.children.get(k));
								}
							}
						}
					}
					odd.clear();
					curDep++;
				}
				else{
					for(int j = 0; j < even.size(); j++){
						HSPIndexNode temp = even.get(j);
						for(int i = 0; i < localRoots.size() && localRoots.get(i).getFirst().getSecond() <= low; i++){
							for(int k = 0; k < temp.children.size();k++){
								if(curDep == low){
									if(((HSPNode) temp.children.get(k)).getPredicate().equals(localRoots.get(i).getFirst().getFirst())){
										temp.children.set(k, new HSPReferenceNode<T,K,R>(temp, (T) ((HSPNode)temp.children.get(k)).getPredicate(), localRoots.get(i).getSecond()));
										localRoots.remove(i--);
										if(localRoots.size()!=0)
											low = localRoots.get(0).getFirst().getSecond();
										break;
									}
									else if(localRoots.size() == 1 || localRoots.get(i+1).getFirst().getSecond() > low){
										if(temp.children.get(k) instanceof HSPLeafNode<?,?,?>){
											//temp.children.set(k, new HSPIndexNode<T,K,R>(temp, (T) ((HSPNode) temp.children.get(k)).getPredicate(),index, low+1, curDep));
										}
										else if(temp.children.get(k) instanceof HSPReferenceNode<?,?,?>)
											continue;
										odd.add((HSPIndexNode<T, K, R>) temp.children.get(k));
									}
								}
								else if(i ==0){
									if(temp.children.get(k) instanceof HSPLeafNode<?,?,?>){
										//temp.children.set(k, new HSPIndexNode<T,K,R>(temp, (T) ((HSPNode) temp.children.get(k)).getPredicate(),index, low, curDep));
									}
									else if(temp.children.get(k) instanceof HSPReferenceNode<?,?,?>)
										continue;
									odd.add((HSPIndexNode<T, K, R>) temp.children.get(k));
								}
							}
						}
					}
					even.clear();
					curDep++;
				}
			}
			odd.clear();
			even.clear();
			HSPNode<T,K,R> nodule = root;
			ArrayList<HSPNode<T,K,R>> stack = new ArrayList<HSPNode<T,K,R>>(); 
			while(!(stack.size() == 0 && nodule == null)){
				if(nodule != null){
					if(nodule instanceof HSPIndexNode<?,?,?>){
						HSPIndexNode<T,K,R> temp = (HSPIndexNode<T,K,R>)nodule;
						context.write(temp, new HSPReferenceNode<T,K,R>());
						for(int i = 0;i < temp.children.size();i++){
							stack.add(temp.children.get(i));
						}
						nodule = stack.remove(0);
					}
					else{
						context.write(new HSPIndexNode<T,K,R>(null, (ArrayList<HSPNode<T,K,R>>)null), (HSPReferenceNode) nodule);
						nodule = null;
					}
				}
				else
					nodule = stack.remove(0);
			}
		}
	}
}
