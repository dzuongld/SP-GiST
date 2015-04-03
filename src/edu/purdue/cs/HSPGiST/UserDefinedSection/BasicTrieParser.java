package edu.purdue.cs.HSPGiST.UserDefinedSection;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import edu.purdue.cs.HSPGiST.AbstractClasses.Parser;
import edu.purdue.cs.HSPGiST.SupportClasses.*;

/**
 * Simple parser used to test tries
 * @author Dan Fortney
 *
 */
public class BasicTrieParser extends Parser<Object, Text, WritableString, CopyWritableLong> {
	
	BasicTrieParser(){
		keyout = WritableString.class;
		valout = CopyWritableLong.class;
		isArrayParse = true;
	}
	private WritableString node = new WritableString("");
	private CopyWritableLong id = new CopyWritableLong(0);
	@Override
	public Pair<WritableString, CopyWritableLong> parse(Object key, Text value) {
		return null;
	}
	@Override
	public ArrayList<Pair<WritableString, CopyWritableLong>> arrayParse(Object key,
			Text value) {
		ArrayList<Pair<WritableString, CopyWritableLong>> returnSet = new ArrayList<Pair<WritableString, CopyWritableLong>>();
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
			
			start = value.find(" str", start);
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
			node.setStr(temp.toString());
			returnSet.add(new Pair<WritableString, CopyWritableLong>(node, id));
		}
		return returnSet;
	}
	@Override
	public Parser<Object, Text, WritableString, CopyWritableLong> clone() {
		return new BasicTrieParser();
	}

}

