package edu.purdue.cs.HSPGiST.UserDefinedSection;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.SupportClasses.WritablePoint;
import edu.purdue.cs.HSPGiST.SupportClasses.WritableRectangle;

/**
 * Basic implementation of a PR quadtree
 * Has some irregular quirks but works for the most part
 * @author Stefan Brinton
 *
 */
public class QuadTree<R> extends HSPIndex<WritableRectangle,WritablePoint,R>{
	QuadTree(){
		numSpaceParts = 4;
		resolution = 50;
		path = PathShrink.LEAF;
		nodeShrink = true;
		samples = new ArrayList<WritablePoint>();
	}
	private static final int RANGE = 1000;
	

	@Override
	public boolean picksplit(
			HSPLeafNode<WritableRectangle, WritablePoint, R> leaf, int level,
			ArrayList<ArrayList<Pair<WritablePoint, R>>> childrenData,
			ArrayList<WritableRectangle> childrenPredicates) {
		if(level == 1){
			WritableRectangle upperLeft = new WritableRectangle(-RANGE,0,RANGE,RANGE);
			WritableRectangle upperRight = new WritableRectangle(0,0,RANGE,RANGE);
			WritableRectangle lowerLeft = new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE);
			WritableRectangle lowerRight = new WritableRectangle(0,-RANGE,RANGE,RANGE);
			childrenPredicates.add(upperLeft);
			childrenPredicates.add(upperRight);
			childrenPredicates.add(lowerLeft);
			childrenPredicates.add(lowerRight);
			for(Pair<WritablePoint, R> p : leaf.keys){
				WritablePoint point = p.getFirst();
				if(point.getX() < 0){
					if(point.getY() < 0)
						childrenData.get(2).add(p);
					else
						childrenData.get(0).add(p);
				}
				else{
					if(point.getY()<0)
						childrenData.get(3).add(p);
					else
						childrenData.get(1).add(p);
				}
			}
			return childrenData.get(0).size() > numSpaceParts || childrenData.get(1).size() > numSpaceParts 
					|| childrenData.get(2).size() > numSpaceParts || childrenData.get(3).size() > numSpaceParts;
		}
		WritableRectangle predic = leaf.getPredicate();
		if(predic == null){
			return false;
		}
		double x = predic.getX();
		double y = predic.getY();
		double h = predic.getH();
		double w = predic.getW();
		WritableRectangle upperLeft = new WritableRectangle(x,y+h/2,h/2,w/2);
		WritableRectangle upperRight = new WritableRectangle(x+w/2,y+h/2,h/2,w/2);
		WritableRectangle lowerLeft = new WritableRectangle(x,y,h/2,w/2);
		WritableRectangle lowerRight = new WritableRectangle(x+w/2,y,h/2,w/2);
		childrenPredicates.add(upperLeft);
		childrenPredicates.add(upperRight);
		childrenPredicates.add(lowerLeft);
		childrenPredicates.add(lowerRight);
		for(Pair<WritablePoint,R> p : leaf.keys){
			if(upperLeft.contains(p.getFirst()))
				childrenData.get(0).add(p);
			else if(upperRight.contains(p.getFirst()))
				childrenData.get(1).add(p);
			else if(lowerLeft.contains(p.getFirst()))
				childrenData.get(2).add(p);
			else
				childrenData.get(3).add(p);
		}
		boolean test0 = childrenData.get(0).size() > numSpaceParts;
		boolean test1 = childrenData.get(1).size() > numSpaceParts;
		boolean test2 = childrenData.get(2).size() > numSpaceParts;
		boolean test3 = childrenData.get(3).size() > numSpaceParts;
		return test0 || test1 || test2 ||test3;  
	}

	@Override
	public WritableRectangle determinePredicate(WritablePoint key,
			WritableRectangle parentPred, int level) {
		WritableRectangle upperLeft;
		WritableRectangle upperRight;
		WritableRectangle lowerLeft;
		WritableRectangle lowerRight;
		if(parentPred != null){
			double x = parentPred.getX();
			double y = parentPred.getY();
			double h = parentPred.getH();
			double w = parentPred.getW();
			upperLeft = new WritableRectangle(x,y+h/2,h/2,w/2);
			upperRight = new WritableRectangle(x+w/2,y+h/2,h/2,w/2);
			lowerLeft = new WritableRectangle(x,y,h/2,w/2);
			lowerRight = new WritableRectangle(x+w/2,y,h/2,w/2);
		}
		else{
			upperLeft = new WritableRectangle(-RANGE,0,RANGE,RANGE);
			upperRight = new WritableRectangle(0,0,RANGE,RANGE);
			lowerLeft = new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE);
			lowerRight = new WritableRectangle(0,-RANGE,RANGE,RANGE);
		}
		if(upperLeft.contains(key))
			return upperLeft;
		else if(upperRight.contains(key))
			return upperRight;
		else if(lowerLeft.contains(key))
			return lowerLeft;
		else
			return lowerRight;
		
	}
	
	@Override
	public boolean consistent(HSPNode<WritableRectangle, WritablePoint,R> e,
			WritablePoint q, int level) {
		return e.getPredicate().contains(q);
	}
	
	@Override
	public boolean consistent(WritableRectangle e, WritablePoint q, int level) {
		return e.contains(q);
	}
	
	@Override
	public void setupPartitions(int numOfReducers) {
		partitionPreds = new ArrayList<Pair<WritableRectangle, Integer>>();
		globalRoot = new HSPIndexNode<WritableRectangle, WritablePoint, R>(null, (WritableRectangle)null);
		int divisions = (numOfReducers - 1)/3;
		WritableRectangle upperLeft = new WritableRectangle(-RANGE,0,RANGE,RANGE);
		WritableRectangle upperRight = new WritableRectangle(0,0,RANGE,RANGE);
		WritableRectangle lowerLeft = new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE);
		WritableRectangle lowerRight = new WritableRectangle(0,-RANGE,RANGE,RANGE);
		if(divisions == 1){
			partitionPreds.add(new Pair<WritableRectangle,Integer>(upperLeft,2));
			partitionPreds.add(new Pair<WritableRectangle,Integer>(upperRight,2));
			partitionPreds.add(new Pair<WritableRectangle,Integer>(lowerLeft,2));
			partitionPreds.add(new Pair<WritableRectangle,Integer>(lowerRight,2));
			globalRoot.children.add(new HSPReferenceNode<WritableRectangle,WritablePoint,R>(globalRoot, upperLeft, new Path("part-r-00000")));
			globalRoot.children.add(new HSPReferenceNode<WritableRectangle,WritablePoint,R>(globalRoot, upperRight, new Path("part-r-00001")));
			globalRoot.children.add(new HSPReferenceNode<WritableRectangle,WritablePoint,R>(globalRoot, lowerLeft, new Path("part-r-00002")));
			globalRoot.children.add(new HSPReferenceNode<WritableRectangle,WritablePoint,R>(globalRoot, lowerRight, new Path("part-r-00003")));
			return;
		}
		ArrayList<HSPLeafNode<WritableRectangle,WritablePoint,R>> lowNodes = new ArrayList<HSPLeafNode<WritableRectangle,WritablePoint,R>>();
		ArrayList<Integer> depths = new ArrayList<Integer>();
		
		lowNodes.add(new HSPLeafNode<WritableRectangle,WritablePoint,R>(globalRoot, upperLeft));
		lowNodes.add(new HSPLeafNode<WritableRectangle,WritablePoint,R>(globalRoot, upperRight));
		lowNodes.add(new HSPLeafNode<WritableRectangle,WritablePoint,R>(globalRoot, lowerLeft));
		lowNodes.add(new HSPLeafNode<WritableRectangle,WritablePoint,R>(globalRoot, lowerRight));
		depths.add(2);
		depths.add(2);
		depths.add(2);
		depths.add(2);
		
		divisions--;
		for(int i = 0; i < samples.size();i++){
			for(int j = 0; j < lowNodes.size();j++)
				if(lowNodes.get(j).getPredicate().contains(samples.get(i))){
					lowNodes.get(j).keys.add(new Pair<WritablePoint,R>(samples.get(i), null));
				}
		}
		@SuppressWarnings("unused")
		int r = 0;
		HSPLeafNode<WritableRectangle, WritablePoint, R> splitter;
		HSPIndexNode<WritableRectangle,WritablePoint, R> splitterIndexed;
		int most;
		int j;
		while(true){
			most = -1;
			j = 0;
			for(int i = 0; i < lowNodes.size(); i++){
				if(lowNodes.get(i).keys.size() > most){
					most = lowNodes.get(i).keys.size();
					j = i;
				}
			}
			
			splitter = lowNodes.get(j);
			
			double x = splitter.getPredicate().getX();
			double y = splitter.getPredicate().getY();
			double h = splitter.getPredicate().getH();
			double w = splitter.getPredicate().getW();
			upperLeft = new WritableRectangle(x,y+h/2,h/2,w/2);
			upperRight = new WritableRectangle(x+w/2,y+h/2,h/2,w/2);
			lowerLeft = new WritableRectangle(x,y,h/2,w/2);
			lowerRight = new WritableRectangle(x+w/2,y,h/2,w/2);
			splitterIndexed = new HSPIndexNode<WritableRectangle, WritablePoint, R>(splitter.getParent(), splitter.getPredicate());
			((HSPIndexNode<WritableRectangle,WritablePoint,R>) splitterIndexed.getParent()).children.add(splitterIndexed);
			lowNodes.set(j, new HSPLeafNode<WritableRectangle, WritablePoint, R>(splitterIndexed, upperLeft));
			lowNodes.add(j+1, new HSPLeafNode<WritableRectangle, WritablePoint, R>(splitterIndexed, upperRight));
			lowNodes.add(j+1, new HSPLeafNode<WritableRectangle, WritablePoint, R>(splitterIndexed, lowerLeft));
			lowNodes.add(j+1, new HSPLeafNode<WritableRectangle, WritablePoint, R>(splitterIndexed, lowerRight));
			depths.set(j, depths.get(j)+1);
			depths.add(j+1, depths.get(j));
			depths.add(j+1, depths.get(j));
			depths.add(j+1, depths.get(j));
			divisions--;
			if(divisions <= 0){
				break;
			}
			int count = splitter.keys.size();
			for(int k = 0; k < count;k++){
				for(int i = 0; i < 4; i++){
					if(lowNodes.get(i+j).getPredicate().contains(splitter.keys.get(k).getFirst()))
						lowNodes.get(i+j).keys.add(splitter.keys.get(j));
				}
			}
		}
		for(int i = 0; i < lowNodes.size(); i++){
			((HSPIndexNode<WritableRectangle,WritablePoint,R>) lowNodes.get(i).getParent()).children
				.add(new HSPReferenceNode<WritableRectangle,WritablePoint,R>(lowNodes.get(i).getParent(), lowNodes.get(i).getPredicate(), new Path(String.format("part-r-%05d", i))));
			partitionPreds.add(new Pair<WritableRectangle, Integer>(lowNodes.get(i).getPredicate(), depths.get(i)));
		}
	}

}