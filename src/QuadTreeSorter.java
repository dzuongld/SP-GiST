import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.SupportClasses.WritablePoint;


public class QuadTreeSorter {
	static public void sort(Path file) throws IOException{
		FileSystem hdfs = file.getFileSystem(new Configuration());
		FSDataInputStream in = hdfs.open(file);
		FSDataOutputStream out = hdfs.create(new Path(file.getParent().toString() + "SortedX.txt"));
		StringBuilder sb = new StringBuilder();
		char c;
		int next = 0;
		ArrayList<Pair<WritablePoint, LongWritable>> list = new ArrayList<Pair<WritablePoint,LongWritable>>();
		double x = 0;
		double y = 0;
		long id = 0;
		try{
			while(true){
				c = (char) in.readByte();
				if(Character.isDigit(c) || c == '-'){
					while(Character.isDigit(c) || c == '-' || c =='.' || c == 'E'){
						sb.append(c);
						c = (char) in.readByte();
					}
					if(next == 0){
						x = Double.parseDouble(sb.toString());
						next++;
					}
					else if(next == 1){
						y = Double.parseDouble(sb.toString());
						next++;
					}
					else if(next == 2){
						id = Long.parseLong(sb.toString());
						Pair<WritablePoint, LongWritable> p = new Pair<WritablePoint, LongWritable>(new WritablePoint(x,y), new LongWritable(id));
						if(list.size() == 0)
							list.add(p);
						else{
							boolean add = false;
							if(p.getFirst().getX() >= list.get(list.size()-1).getFirst().getX()){
								list.add(p);
								add = true;
							}
							for(int i = 0; i<list.size() && !add;i++){
								if(p.getFirst().getX() < list.get(i).getFirst().getX()){
									list.add(i, p);
									add =true;
									break;
								}
							}
								
						}
						next = 0;
					}
					sb.setLength(0);
				}
			}
		}catch(EOFException e){
			
		}
		for(Pair p : list)
			out.writeBytes(p.toString()+"\n");
		in.close();
		out.close();
		hdfs.close();
	}
	public static void main(String args[]) throws IllegalArgumentException, IOException{
		
		sort(new Path("Validation/part-r-00000"));
		sort(new Path("Results/QueryResult.txt"));
	}
}
