package edu.purdue.cs.HSPGiST.MapReduceJobs;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

public class GlobalIndexConstructor extends Configured implements Tool{
	@SuppressWarnings("rawtypes")
	private HSPIndex index;
	@SuppressWarnings("rawtypes")
	private Parser parser;
	@SuppressWarnings("rawtypes")
	public GlobalIndexConstructor(HSPIndex index, Parser parse) {
		this.index = index;
		this.parser = parse;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int run(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path globalOutput = new Path(CommandInterpreter.CONSTRUCTSECONDOUT + "-" + parser.getClass().getSimpleName() + "-" + index.getClass().getSimpleName() + "-" + args[3]);
		if(hdfs.exists(globalOutput)){
			hdfs.delete(globalOutput, true);
		}
		hdfs.mkdirs(globalOutput);
		Path globalIndexFile = new Path(globalOutput.toString() + "/GlobalTree");
		FSDataOutputStream output = hdfs.create(globalIndexFile);
		HSPNode nodule = index.globalRoot;
		ArrayList<HSPNode> stack = new ArrayList<HSPNode>(); 
		while(!(stack.size() == 0 && nodule == null)){
			if(nodule != null){
				if(nodule instanceof HSPIndexNode<?,?,?>){
					HSPIndexNode temp = (HSPIndexNode)nodule;
					output.writeBoolean(true);
					temp.write(output);
					for(int i = 0;i < temp.getChildren().size();i++){
						stack.add((HSPNode) temp.getChildren().get(i));
					}
					nodule = stack.remove(0);
				}
				else{
					output.writeBoolean(false);
					((HSPReferenceNode) nodule).write(output);
					nodule = null;
				}
			}
			else
				nodule = stack.remove(0);
		}
		output.close();
		hdfs.close();
		return 0;
	}

}
