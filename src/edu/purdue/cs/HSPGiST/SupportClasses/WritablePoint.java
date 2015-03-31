package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

/**
 * WritableComparable Point class
 * 
 * @author Stefan Brinton
 *
 */
public class WritablePoint implements WritableComparable<WritablePoint>,
		Copyable<WritablePoint> {
	private double x;
	private double y;

	/**
	 * Default Constructor
	 */
	public WritablePoint() {
	}

	/**
	 * Make a writable point
	 * 
	 * @param x
	 *            The x value of the point
	 * @param y
	 *            The y value of the point
	 */
	public WritablePoint(double x, double y) {
		this.y = y;
		this.x = x;
	}

	/*
	 * Getters and setters
	 */

	public void setX(double x) {
		this.x = x;
	}

	public double getX() {
		return x;
	}

	public void setY(double y) {
		this.y = y;
	}

	public double getY() {
		return y;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof WritablePoint))
			return false;
		WritablePoint other = (WritablePoint) o;
		return this.x == other.x && this.y == other.y;
	}

	@Override
	public int compareTo(WritablePoint o) {
		if (equals(o))
			return 0;
		double x1 = this.x;
		double x2 = o.x;
		int ret = (x1 < x2 ? -1 : (x1 == x2 ? 0 : 1));
		if (ret == 0)
			return y < o.y ? -1 : 1;
		return ret;
	}

	@Override
	public int hashCode() {
		return Objects.hash(x, y);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("X: ");
		return sb.append(Double.toString(x)).append(" Y: ")
				.append(Double.toString(y)).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	@Override
	public WritablePoint clone() {
		return new WritablePoint(x, y);
	}

	@Override
	public WritablePoint copy() {
		return clone();
	}
}