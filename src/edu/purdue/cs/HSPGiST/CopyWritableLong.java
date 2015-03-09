package edu.purdue.cs.HSPGiST;

import org.apache.hadoop.io.LongWritable;

/**
 * Quick and Dirty implementation of a copyable LongWritable for debug
 * @author Stefan Brinton
 *
 */
public class CopyWritableLong extends LongWritable implements Copyable<CopyWritableLong>{
	CopyWritableLong(){
		super();
	}
	CopyWritableLong(LongWritable l){
		super(l.get());
	}
	CopyWritableLong(long l){
		super(l);
	}
	@Override
	public CopyWritableLong copy() {
		return new CopyWritableLong(get());
	}
	
}
