package com.spark.smartchair;

import java.io.Serializable;

public	class Posture implements Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1623813949032944816L;
		private int userId = 1;
		private int chairId = 1;
		private long duration;
		private long time;
		private int postureId;
		private String postureGrade;
		private String reco;
		public Posture(){}
		public int getUserId() {
			return userId;
		}
		public void setUserId(int userId) {
			this.userId = userId;
		}
		public int getChairId() {
			return chairId;
		}
		public void setChairId(int chairId) {
			this.chairId = chairId;
		}
		public long getDuration() {
			return duration;
		}
		public void setDuration(long duration) {
			this.duration = duration;
		}
		public long getTime() {
			return time;
		}
		public void setTime(long time) {
			this.time = time;
		}
		public int getPostureId() {
			return postureId;
		}
		public void setPostureId(int postureId) {
			this.postureId = postureId;
		}
		public String getReco() {
			return reco;
		}
		public void setReco(String reco) {
			this.reco = reco;
		}
		public String getPostureGrade() {
			return postureGrade;
		}
		public void setPostureGrade(String postureGrade) {
			this.postureGrade = postureGrade;
		}
		@Override
		public boolean equals(Object p) {
			if(p==null || ! (p instanceof Posture)) return false;
			return ((Posture)p).getPostureId() == this.getPostureId();
		}
		@Override
		public int hashCode() {
			return this.postureId;
		}
	}
