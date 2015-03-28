package edu.purdue.cs.HSPGiST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

/**
 * Quick and dirty implementation of a writecomparable rectangle for debug usage
 * @author Stefan Brinton
 *
 */
public class WritableRectangle implements WritableComparable<WritableRectangle>{
	private double x;		
	private double y;
	private double h;
	private double w;
	public WritableRectangle() {}

	public WritableRectangle(double x, double y, double h, double w){
		this.y = y;
		this.h = h;
		this.x = x;
		this.w = w;
		if(h < 0){
			this.y = this.y + h;
			this.h = -h;
		}
		if(w < 0){
			this.x = this.x + w;
			this.w = -w;
		}
	}
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
		h = in.readDouble();
		w = in.readDouble();
	}
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(h);
		out.writeDouble(w);
	}
	public double getX(){
		return x;
	}
	public double getY(){
		return y;
	}
	public double getH(){
		return h;
	}
	public double getW(){
		return w;
	}
	public int hashCode() {
		//TODO: rewrite hashcode to be a proper hashcode
		return (int) (x*y-h+w); 
	}
	public boolean contains(WritablePoint p){
		return x < p.getX() && p.getX() < x+w && y < p.getY() && p.getY() < y+h;
	}
	public int containsNumber(ArrayList<WritablePoint> list){
		int count = 0;
		for(WritablePoint p : list)
			if(contains(p))
				count++;
		return count;
	}
	public boolean equals(Object o){
		if(!(o instanceof WritableRectangle))
			return false;
		WritableRectangle other = (WritableRectangle) o;
		return this.x == other.x && this.y == other.y && this.h == other.h && this.w == other.w;
	}
	public int compareTo(WritableRectangle o){
		double x1 = this.x;
		double x2 = o.x;
		return (x1<x2 ? -1 : (x1==x2 ? 0 : 1));
	}
	public String toString() {
		return "X: " + Double.toString(x) + " Y: " + Double.toString(y) + " H: " + Double.toString(h)+ " W: " + Double.toString(w);
	}
}

