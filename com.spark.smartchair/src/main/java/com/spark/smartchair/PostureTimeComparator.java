package com.spark.smartchair;

import java.io.Serializable;
import java.util.Comparator;

public class PostureTimeComparator implements Serializable, Comparator<Posture> {

	@Override
	public int compare(Posture o1, Posture o2) {
		// TODO Auto-generated method stub
		if(o1.getTime() < o2.getTime()) {
			return -1;
		}
		else if(o1.getTime() > o2.getTime()) {
			return 1;
		}
		return 0;
	}

}
