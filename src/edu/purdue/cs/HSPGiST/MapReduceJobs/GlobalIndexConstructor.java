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

/**
 * Not actually a MapReduce Job, this tool just outputs the global tree
 * constructed during Local Index Construction to file
 * 
 * @author Stefan Brinton
 *
 */
public class GlobalIndexConstructor extends Configured implements Tool {
	@SuppressWarnings("rawtypes")
	private HSPIndex index;

	@SuppressWarnings("rawtypes")
	public GlobalIndexConstructor(HSPIndex index, Parser parse) {
		this.index = index;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int run(String[] args) throws Exception {
		// Get HDFS
		FileSystem hdfs = FileSystem.get(new Configuration());
		// Construct Path name and initialize path
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTSECONDOUT);
		Path globalOutput = new Path(sb.append(CommandInterpreter.postScript)
				.toString());
		// Clear out pre-existing files if applicable
		if (hdfs.exists(globalOutput)) {
			hdfs.delete(globalOutput, true);
		}
		hdfs.mkdirs(globalOutput);
		// Create global output file
		Path globalIndexFile = new Path(sb.append("/")
				.append(CommandInterpreter.GLOBALFILE).toString());
		FSDataOutputStream output = hdfs.create(globalIndexFile);
		// Print nodes in level order to file
		HSPNode nodule = index.globalRoot;
		ArrayList<HSPNode> stack = new ArrayList<HSPNode>();
		while (!(stack.size() == 0 && nodule == null)) {
			if (nodule != null) {
				if (nodule instanceof HSPIndexNode<?, ?, ?>) {
					HSPIndexNode temp = (HSPIndexNode) nodule;
					// Since this isn't a map-reduce job and won't be read by
					// one either,
					// we need to put boolean sentinels to mark if this node is
					// an index node or a reference node
					output.writeBoolean(true);
					temp.write(output);
					for (int i = 0; i < temp.getChildren().size(); i++) {
						stack.add((HSPNode) temp.getChildren().get(i));
					}
					nodule = stack.remove(0);
				} else {
					//See above comment
					output.writeBoolean(false);
					((HSPReferenceNode) nodule).write(output);
					nodule = null;
				}
			} else
				nodule = stack.remove(0);
		}
		//Close and return success
		output.close();
		hdfs.close();
		return 0;
	}

}
