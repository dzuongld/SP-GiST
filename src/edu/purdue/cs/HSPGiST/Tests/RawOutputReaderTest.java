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

public class RawOutputReaderTest extends Configured implements Tool{
	@SuppressWarnings("rawtypes")
	public int run(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path globalIndexFile = new Path("Global-OSMParser-QuadTree-input/GlobalTree");
		FSDataInputStream input = hdfs.open(globalIndexFile);
		FSDataOutputStream output = hdfs.create(new Path("TestOutput/RawAsText.txt"));
		HSPIndexNode inIndex;
		HSPReferenceNode inRef;
		while(input.available() != 0){
			
			if(input.readBoolean()){
				inIndex = new HSPIndexNode();
				inIndex.readFields(input);
				output.writeBytes(inIndex.toString() + "\n");
			}
			else{
				inRef = new HSPReferenceNode();
				inRef.readFields(input);
				output.writeBytes(inRef.toString() + "\n");
			}
		}
		output.close();
		hdfs.close();
		return 0;
	}
}
