package com.spark.smartchair;

public 	class Sensor {
	private int pressure_one;private int pressure_two;private int pressure_three;private int pressure_four;
	private int distance_one; private int distance_two; private int angle;private long time; public Sensor(){}

	public int getPressure_one() {
		return pressure_one;
	}

	public void setPressure_one(int pressure_one) {
		this.pressure_one = pressure_one;
	}

	public int getPressure_two() {
		return pressure_two;
	}

	public void setPressure_two(int pressure_two) {
		this.pressure_two = pressure_two;
	}

	public int getPressure_three() {
		return pressure_three;
	}

	public void setPressure_three(int pressure_three) {
		this.pressure_three = pressure_three;
	}

	public int getPressure_four() {
		return pressure_four;
	}

	public void setPressure_four(int pressure_four) {
		this.pressure_four = pressure_four;
	}

	public int getDistance_one() {
		return distance_one;
	}

	public void setDistance_one(int distance_one) {
		this.distance_one = distance_one;
	}

	public int getDistance_two() {
		return distance_two;
	}

	public void setDistance_two(int distance_two) {
		this.distance_two = distance_two;
	}

	public int getAngle() {
		return angle;
	}

	public void setAngle(int angle) {
		this.angle = angle;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
	
	
}