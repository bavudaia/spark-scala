package com.spark.smartchair;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class TupleComparator implements Serializable , Comparator<Tuple2<Posture, Integer>>{

	@Override
	public int compare(Tuple2<Posture, Integer> o1, Tuple2<Posture, Integer> o2) {
		// TODO Auto-generated method stub
		if(o1._2() < o2._2()) {
			return -1;
		}
		if(o1._2() > o2._2()) {
			return 1;
		}
		return 0;
	}

}
