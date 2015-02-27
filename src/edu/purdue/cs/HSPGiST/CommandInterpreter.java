package edu.purdue.cs.HSPGiST;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

/**
 * The command interpreter is the main of HSP-GiST
 * It takes user input to run one of the Jobs/Jobsets
 * It requires updating to add user classes to allow expedient
 * command line use 
 * @author Stefan Brinton & Daniel Fortney
 *
 */
public class CommandInterpreter {
	@SuppressWarnings("rawtypes")
	public static void main(String args[]) throws Exception{
		//This switch determines the SP tree type
		//Add cases for new tree types
		HSPIndex index;
		switch(args[1]){
		case "Grid":
			index = new GridTree();
			OpRunner<WritablePoint, WritableRectangle> runner = new OpRunner<WritablePoint, WritableRectangle>();
			runner.runOp(index,args);
		}
		//This switch statement determines the parser
		//Add cases for your parsers to add them
		
		
	}
	//Offers extra level of indirection to allow parameterization of predicates to allow different parsers
	//to work with the same index
	public static class OpRunner<K,T>{
		public void runOp(HSPIndex<K,T> index, String args[]) throws Exception{
			//Determines the operation to run
			// DO NOT MODIFY
			switch (args[0]) {
			case "build":
				Parser parse;
				LocalIndexConstructor construct = null;
				switch(args[2]){
				case "OSM":
					parse = new OSMParser();
					construct = new LocalIndexConstructor<Object,Text,K,Text,T>(parse, index);
				}
				if(construct == null){
					System.err.println("Failed to provide a valid parser on build instruction");
					System.exit(-1);
				}
				System.exit(ToolRunner.run(construct, args));
			}
		}
	}
}
