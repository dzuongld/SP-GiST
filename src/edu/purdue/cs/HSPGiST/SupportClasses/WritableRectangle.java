package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import edu.purdue.cs.HSPGiST.AbstractClasses.Predicate;

/**
 * Implementation of WritableRectangles
 * 
 * @author Stefan Brinton
 *
 */
public class WritableRectangle extends Predicate {
	private double x;
	private double y;
	private double h;
	private double w;

	/**
	 * Default Constructor
	 */
	public WritableRectangle() {
	}

	/**
	 * Setups a rectangle with the given parameters
	 * 
	 * @param x
	 *            The lower left corner of the rectangle's x value
	 * @param y
	 *            The lower left corner of the rectangle's y value
	 * @param w
	 *            The width of the rectangle
	 * @param h
	 *            The height of the rectangle
	 */
	public WritableRectangle(double x, double y, double w, double h) {
		this.y = y;
		this.h = h;
		this.x = x;
		this.w = w;
		if (h < 0) {
			this.y = this.y + h;
			this.h = -h;
		}
		if (w < 0) {
			this.x = this.x + w;
			this.w = -w;
		}
	}

	/**
	 * Public getter methods
	 */

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public double getH() {
		return h;
	}

	public double getW() {
		return w;
	}

	/**
	 * Public Setter methods
	 */

	public void setX(double x) {
		this.x = x;
	}

	public void setY(double y) {
		this.y = y;
	}

	public void setH(double h) {
		this.h = h;
	}

	public void setW(double w) {
		this.w = w;
	}

	/**
	 * Check if a point lies within a rectangle's bounds
	 * 
	 * @param p
	 *            The point to check
	 * @return True if a point is within a rectangle
	 */
	public boolean contains(WritablePoint p) {
		if (p == null)
			return false;
		return x <= p.getX() && p.getX() <= x + w && y <= p.getY()
				&& p.getY() <= y + h;
	}

	/**
	 * Checks if a given rectangle is bounded by another Does not return true
	 * for equivalent rectangles
	 * 
	 * @param r
	 *            The rectangle that may be a subspace of this one
	 * @return True if r is contained within this one (may share a common lower
	 *         left corner)
	 */
	public boolean contains(WritableRectangle r) {
		if (r == null)
			return false;
		if (x <= r.x && y <= r.y && h > r.h && w > r.w)
			return true;
		return false;
	}

	public boolean overlaps(WritableRectangle r){
		WritablePoint p1 = new WritablePoint(x, y);
		WritablePoint p2 = new WritablePoint(x + w, y);
		WritablePoint p3 = new WritablePoint(x, y + h);
		WritablePoint p4 = new WritablePoint(x + w, y + h);
		boolean ret = r.contains(p1) || r.contains(p2) || r.contains(p3)
				|| r.contains(p4);
		if (ret)
			return ret;
		p1 = new WritablePoint(r.getX(), r.getY());
		p2 = new WritablePoint(r.getX() + r.getW(), r.getY());
		p3 = new WritablePoint(r.getX(), r.getY() + r.getH());
		p4 = new WritablePoint(r.getX() + r.getW(), r.getY() + r.getH());
		return contains(p1) || contains(p2) || contains(p3) || contains(p4);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof WritableRectangle))
			return false;
		WritableRectangle other = (WritableRectangle) o;
		return this.x == other.x && this.y == other.y && this.h == other.h
				&& this.w == other.w;
	}

	@Override
	public int compareTo(Predicate otter) {
		if(!(otter instanceof WritableRectangle))
			return -1;
		WritableRectangle o = (WritableRectangle) otter;
		if (equals(o))
			return 0;
		int ret = h * w < o.w * o.h ? -1 : (h * w == o.h * o.w ? 0 : 1);

		if (ret == 0) {
			ret = x < o.x ? -1 : (x == o.x ? 0 : 1);
			if (ret == 0)
				ret = y < o.y ? -1 : (y == o.y ? 0 : 1);
		}
		return ret;
	}

	@Override
	public int hashCode() {
		return Objects.hash(x, y, h, w);
	}

	@Override
	public String toString() {
		return "X: " + Double.toString(x) + " Y: " + Double.toString(y)
				+ " H: " + Double.toString(h) + " W: " + Double.toString(w);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
		h = in.readDouble();
		w = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeDouble(h);
		out.writeDouble(w);
	}

	@Override
	public WritableRectangle copy() {
		return new WritableRectangle(x, y, w, h);
	}

	@Override
	public long getSize() {
		//Divide by 8 for bits to bytes * 4 for 4 doubles
		return Double.SIZE>>1;
	}

	@Override
	public String convertToJSON() {
		StringBuilder sb = new StringBuilder("[");
		sb.append(x);
		sb.append(',');
		sb.append(y);
		sb.append(',');
		sb.append(w);
		sb.append(',');
		sb.append(h);
		sb.append(']');
		return sb.toString();
	}

	@Override
	public void convertFromJSON(String input) {
		if(!input.startsWith("[")){
			//We are trying to parse something that isn't right
			return;
		}
		String clean = input.substring(1, input.length()-1);
		String[] fields = clean.split(",");
		x = Double.parseDouble(fields[0]);
		y = Double.parseDouble(fields[1]);
		w = Double.parseDouble(fields[2]);
		h = Double.parseDouble(fields[3]);
	}
}
