package edu.purdue.cs.HSPGiST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class WritablePoint implements WritableComparable<WritablePoint>{
	private double x;
	private double y;
	public WritablePoint() {}

	public WritablePoint(double x, double y){
		this.y = y;
		this.x = x;
	}
	public void setX(double x){
		this.x = x;
	}
	public double getX(){
		return x;
	}
	public void setY(double y){
		this.y = y;
	}
	public double getY(){
		return y;
	}
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
	}
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}
	public int hashCode() {
		//I don't really care about a high collision rate
		return (int) (x*y); 
	}
	public boolean equals(Object o) {
		if(!(o instanceof WritablePoint))
			return false;
		WritablePoint other = (WritablePoint)o;
		return this.x == other.x && this.y == other.y;
	}
	public int compareTo(WritablePoint o){
		double x1 = this.x;
		double x2 = o.x;
		return (x1<x2 ? -1 : (x1==x2 ? 0 : 1));
	}
	public String toString() {
		return "Lon: " + Double.toString(x) + " Lat: "+ Double.toString(y);
	}
	public WritablePoint clone(){
		return new WritablePoint(x,y);
	}
}