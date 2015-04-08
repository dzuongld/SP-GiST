package edu.purdue.cs.HSPGiST.UserDefinedSection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.SupportClasses.*;
import edu.purdue.cs.HSPGiST.Tasks.GlobalIndexConstructor;
import edu.purdue.cs.HSPGiST.Tasks.LocalIndexConstructor;
import edu.purdue.cs.HSPGiST.Tasks.RandomSample;
import edu.purdue.cs.HSPGiST.Tasks.TreeSearcher;

/**
 * The command interpreter is the main of HSP-GiST It takes user input to run
 * one of the Jobs/Jobsets It requires updating to add user classes to allow
 * expedient command line use
 * 
 * @author Stefan Brinton & Daniel Fortney
 *
 */
public class CommandInterpreter {
	/**
	 * The prefix for the output directory for LocalIndexConstuctor
	 */
	public final static String CONSTRUCTFIRSTOUT = "Local";

	/**
	 * The prefix for the output directory for GlobalIndexConstructor
	 */
	public static final String CONSTRUCTSECONDOUT = "Global";

	/**
	 * The suffix for the output directories
	 */
	public static String postScript = "";

	public static final String GLOBALFILE = "GlobalTree";

	public static final String USEHELP = "Invalid Input: use the -h or -help flag for a list of uses";
	public static final String HELP = "HSP-GiST <Option>\nFlags Usage:\n";
	public static final String BUSAGE = "Usage for -b or -build:\nHSP-GiST -b(uild) <Index_Name> <Parser_Name> <Input_Directory> [Sampling_Percentage]";
	public static final String QUSAGE = "Usage for -q or -query:\nHSP-GiST -q(uery) <Index_Name> <Parser_Name> <Input_Directory> <Index_Range>";
	public static final String QQOSM = "Usage for -q or -query:\nHSP-GiST -q(uery) <Index_Name> <Parser_Name> <Input_Directory> <X Value of Lower Left Corner> <Y Value of Lower Left Corner> <X Value of Upper Right Corner> <Y Value Of Upper Right Corner>";
	public static final String QTTRIE = "Usage for -q or -query:\nHSP-GiST -q(uery) <Index_Name> <Parser_Name> <Input_Directory> <First String Alphabetically> <Second String Alphabetically>";
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String args[]) throws Exception {
		// Determines the operation to run
		// DO NOT MODIFY
		HSPIndex index = null;
		StringBuilder sb = null;
		if(args.length == 0){
			System.out.println(USEHELP);
			System.exit(1);
		}
		switch (args[0]) {
		case "-build":
		case "-b":
			if(args.length != 4 && args.length != 5){
				System.out.println(BUSAGE);
				System.exit(1);
			}
			String arg[] = args[3].split("/");
			Parser parse = null;
			LocalIndexConstructor construct = null;
			GlobalIndexConstructor finish = null;
			RandomSample sampler = null;
			// This switch statement determines the parser
			// Add cases for your parsers to add them
			switch (args[2]) {
			case "OSM":
				index = makeIndex(args[1], new CopyWritableLong());
				parse = new OSMParser();
				sampler = new RandomSample<Object, Text, WritablePoint, CopyWritableLong>(
						parse, index);
				construct = new LocalIndexConstructor<Object, Text, WritablePoint, CopyWritableLong, WritableRectangle>(
						parse, index);
				sb = new StringBuilder("-");
				
				
				postScript = sb.append(parse.getClass().getSimpleName())
						.append("-").append(index.getClass().getSimpleName())
						.append("-").append(arg[arg.length-1]).toString();
				break;
			case "BasicTrie":
				index = makeIndex(args[1], new CopyWritableLong());
				parse = new BasicTrieParser();
				sampler = new RandomSample<Object, Text, WritableString, Text>(
						parse, index);
				construct = new LocalIndexConstructor<Object, Text, WritableString, CopyWritableLong, WritableChar>(
						parse, index);
				sb = new StringBuilder("-");
				postScript = sb.append(parse.getClass().getSimpleName())
						.append("-").append(index.getClass().getSimpleName())
						.append("-").append(arg[arg.length-1]).toString();
				break;
			}
			if (construct == null) {
				System.err
						.println("Failed to provide a valid parser on build instruction");
				System.exit(1);
			}
			int check = ToolRunner.run(sampler, args);
			if (check == 1) {
				System.err.println("The Sampler has failed to sample data");
				System.exit(1);
			}
			System.exit(ToolRunner.run(construct, args));
			break;
		case "-q":
		case "-query":
			if(args.length < 3){
				System.out.println(QUSAGE);
			}
			String arg1[] = args[3].split("/");
			index = null;
			sb = null;
			TreeSearcher search = null;
			switch (args[2]) {
			case "OSM":
				parse = new OSMParser();
				index = makeIndex(args[1], new CopyWritableLong());
				search = makeSearcher(args, index, new CopyWritableLong());
				sb = new StringBuilder("-");
				
				postScript = sb.append(parse.getClass().getSimpleName())
						.append("-").append(index.getClass().getSimpleName())
						.append("-").append(arg1[arg1.length-1]).toString();
				break;
			case "BasicTrie":
				index = makeIndex(args[1], new LongWritable());
				parse = new BasicTrieParser();
				search = makeSearcher(args, index, new CopyWritableLong());
				sb = new StringBuilder("-");
				postScript = sb.append(parse.getClass().getSimpleName())
						.append("-").append(index.getClass().getSimpleName())
						.append("-").append(arg1[arg1.length-1]).toString();
				break;
			}
			System.exit(ToolRunner.run(search, args));
		case "-h":
		case "-help":
			System.out.println(HELP);
		}
	}

	/**
	 * This method encapsulates index creation so to expedite user addition of
	 * parsers and index types
	 * 
	 * @param arg
	 *            This argument is args[1], i.e. the name of the index type
	 * @param infer
	 *            This argument is used to let the method infer the type of the
	 *            sp tree
	 * @return An new instance of that index type
	 */
	@SuppressWarnings("rawtypes")
	public static <R> HSPIndex makeIndex(String arg, R infer) {
		// This switch statement determines the index type
		// Add cases for your parsers to add them
		switch (arg) {
		case "Quad":
			return new QuadTree<R>();
		case "Trie":
			return new Trie<R>();
		}
		System.err.println("Failed to provide a valid index tree type");
		System.exit(-1);
		return null;
	}
		
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <R> TreeSearcher makeSearcher(String[] args, HSPIndex index, R infer) {
		// This switch statement determines the index type
		// Add cases for your parsers to add them
		switch (args[1]) {
		case "Quad":
			if(args.length != 8){
				System.out.println(QQOSM);
				System.exit(-1);
			}
			WritablePoint p1 = null;
			WritablePoint p2 = null;
			try{
				double x1 = Double.parseDouble(args[4]);
				double y1 = Double.parseDouble(args[5]);
				double x2 = Double.parseDouble(args[6]);
				double y2 = Double.parseDouble(args[7]);
				p1 = new WritablePoint(x1,y1);
				p2 = new WritablePoint(x2,y2);
			}catch(Exception e){
				System.out.println("Invalid value supplied for coordinate\n");
				System.exit(1);
			}
			return new TreeSearcher<WritableRectangle, WritablePoint, R>(p1, p2, index);
		case "Trie":
			if(args.length != 6){
				System.out.println(QTTRIE);
				System.exit(-1);
			}
			return new TreeSearcher<WritableChar, WritableString, R>(new WritableString(args[4]),new WritableString(args[5]), index);
		}
		System.err.println("Failed to provide a valid index tree type");
		System.exit(-1);
		return null;
	}
}
