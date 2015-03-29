package edu.purdue.cs.HSPGiST.UserDefinedSection;

import java.util.ArrayList;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
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
	public boolean consistent(HSPNode<WritableRectangle, WritablePoint,R> e,
			WritablePoint q, int level) {
		return e.getPredicate().contains(q);
	}

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
	public boolean consistent(WritableRectangle e, WritablePoint q, int level) {
		return e.contains(q);
	}
	@Override
	public void setupPartitions(int numOfReducers) {
		partitionPreds = new ArrayList<Pair<WritableRectangle, Integer>>();
		if(numOfReducers == 1){
			partitionPreds.add(new Pair<WritableRectangle, Integer>(null, 1));
			return;
		}
		int divisions = (numOfReducers - 1)/3;
		ArrayList<WritableRectangle> preds = new ArrayList<WritableRectangle>();
		ArrayList<ArrayList<WritablePoint>> keys = new ArrayList<ArrayList<WritablePoint>>();
		ArrayList<Integer> depths = new ArrayList<Integer>();
		preds.add(new WritableRectangle(-RANGE,0,RANGE,RANGE));
		preds.add(new WritableRectangle(0,0,RANGE,RANGE));
		preds.add(new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE));
		preds.add(new WritableRectangle(0,-RANGE,RANGE,RANGE));
		depths.add(2);
		depths.add(2);
		depths.add(2);
		depths.add(2);
		if(divisions == 1){
			partitionPreds.add(new Pair<WritableRectangle,Integer>(preds.get(0),2));
			partitionPreds.add(new Pair<WritableRectangle,Integer>(preds.get(1),2));
			partitionPreds.add(new Pair<WritableRectangle,Integer>(preds.get(2),2));
			partitionPreds.add(new Pair<WritableRectangle,Integer>(preds.get(3),2));
			return;
		}
		divisions--;
		for(int j = 0; j < preds.size();j++){
			keys.add(new ArrayList<WritablePoint>());
		}
		for(int i = 0; i < samples.size();i++){
			for(int j = 0; j < preds.size();j++)
				if(preds.get(j).contains(samples.get(i))){
					keys.get(j).add(samples.get(i));
				}
		}
		while(true){
			int most = -1;
			int j = 0;
			for(int i = 0; i < keys.size(); i++){
				if(keys.get(i).size() > most){
					most = keys.get(i).size();
					j = i;
				}
			}
			WritableRectangle predic = preds.get(j);
			double x = predic.getX();
			double y = predic.getY();
			double h = predic.getH();
			double w = predic.getW();
			preds.set(j, new WritableRectangle(x,y+h/2,h/2,w/2));
			preds.add(j+1, new WritableRectangle(x+w/2,y+h/2,h/2,w/2));
			preds.add(j+1, new WritableRectangle(x,y,h/2,w/2));
			preds.add(j+1, new WritableRectangle(x+w/2,y,h/2,w/2));
			depths.set(j, depths.get(j)+1);
			depths.add(j+1, depths.get(j));
			depths.add(j+1, depths.get(j));
			depths.add(j+1, depths.get(j));
			divisions--;
			if(divisions <= 0){
				break;
			}
			keys.add(j+1,new ArrayList<WritablePoint>());
			keys.add(j+1,new ArrayList<WritablePoint>());
			keys.add(j+1,new ArrayList<WritablePoint>());
			keys.add(j+1,new ArrayList<WritablePoint>());
			int count = keys.get(j).size();
			for(int k = 0; k < count;k++){
				for(int i = 0; i < 4; i++){
					if(preds.get(i+j).contains(keys.get(j).get(k)))
						keys.get(i+j+1).add(keys.get(j).get(k));
				}
			}
			keys.remove(j);
		}
		for(int i = 0; i < preds.size(); i++){
			partitionPreds.add(new Pair<WritableRectangle, Integer>(preds.get(i), depths.get(i)));
		}
	}
}