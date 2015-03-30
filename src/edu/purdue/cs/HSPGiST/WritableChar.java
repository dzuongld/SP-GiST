package edu.purdue.cs.HSPGiST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Simple writablechar implementation. Only represents the 26 lowercase letters.
 * @author Dan Fortney and Stefan Brinton
 *
 */
public class WritableChar implements WritableComparable<WritableChar>, Copyable<WritableChar>{
	private char ch;
	public WritableChar() {}

	public WritableChar(char input){
		if (java.lang.Character.isLowerCase(input)) {
			ch = input;
		}else if (java.lang.Character.isUpperCase(input)){
			ch = java.lang.Character.toLowerCase(input);
		}else{
			ch = '-'; //the "blank" or terminating character
		}
	}
	public void setChar(char input){
		this.ch = input;
	}
	public char getChar(){
		return ch;
	}
	public void readFields(DataInput in) throws IOException {
		ch = in.readChar();
	}
	public void write(DataOutput out) throws IOException {
		out.writeChar(ch);
	}
	public int hashCode() {
		//Do we need a hash code here?
		return (int) (ch); 
	}
	public boolean equals(Object o) {
		if(!(o instanceof WritableChar))
			return false;
		WritableChar other = (WritableChar)o;
		return this.ch == other.ch;
	}
	public int compareTo(WritableChar o){
		char ch1 = this.ch;
		char ch2 = o.ch;
		return (ch1<ch2 ? -1 : (ch1==ch2 ? 0 : 1));
	}
	public String toString() {
		return "Ch: " + ch;
	}
	public WritableChar clone(){
		return new WritableChar(ch);
	}
	public WritableChar copy(){
		return clone();
	}
}
