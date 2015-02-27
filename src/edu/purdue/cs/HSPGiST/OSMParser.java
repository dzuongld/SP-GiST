package edu.purdue.cs.HSPGiST;

import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class OSMParser extends Parser<Object, Text, WritablePoint, LongWritable> {
	
	OSMParser(){
		keyout = WritablePoint.class;
		valout = LongWritable.class;
		isArrayParse = true;
	}
	private WritablePoint node = new WritablePoint(0,0);
	private LongWritable id = new LongWritable(0);
	@Override
	public Pair<WritablePoint, LongWritable> parse(Object key, Text value) {
		return null;
	}
	@Override
	public ArrayList<Pair<WritablePoint, LongWritable>> arrayParse(Object key,
			Text value) {
		ArrayList<Pair<WritablePoint, LongWritable>> returnSet = new ArrayList<Pair<WritablePoint, LongWritable>>();
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
			returnSet.add(new Pair<WritablePoint, LongWritable>(node, id));
		}
		return returnSet;
	}
	@Override
	public Parser<Object, Text, WritablePoint, LongWritable> clone() {
		return new OSMParser();
	}

}
