package edu.purdue.cs.HSPGiST.Tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.UserDefinedSection.CommandInterpreter;

/**
 * Trivial Class that will convert output from the GlobalIndexConstructor to
 * text
 * 
 * @author Stefan Brinton
 *
 */
public class GlobalBinaryReader extends Configured implements Tool {
	@SuppressWarnings("rawtypes")
	public int run(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		StringBuilder sb = new StringBuilder(
				CommandInterpreter.CONSTRUCTSECONDOUT);

		Path globalIndexFile = new Path(sb
				.append(CommandInterpreter.postScript).append("/")
				.append(CommandInterpreter.GLOBALFILE).toString());
		FSDataInputStream input = hdfs.open(globalIndexFile);
		FSDataOutputStream output = hdfs.create(new Path(
				"TestOutput/RawAsText.txt"));
		HSPIndexNode inIndex;
		HSPReferenceNode inRef;
		while (input.available() != 0) {

			if (input.readBoolean()) {
				inIndex = new HSPIndexNode();
				inIndex.readFields(input);
				output.writeBytes(inIndex.toString() + "\n");
			} else {
				inRef = new HSPReferenceNode();
				inRef.readFields(input);
				output.writeBytes(inRef.toString() + "\n");
			}
		}
		input.close();
		output.close();
		hdfs.close();
		return 0;
	}
}
