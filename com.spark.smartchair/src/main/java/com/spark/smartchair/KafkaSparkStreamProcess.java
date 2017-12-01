package com.spark.smartchair;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.gson.JsonObject;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaSparkStreamProcess implements Serializable{
	
	private static final long serialVersionUID = -8769285377428486688L;
	private static final int INTERVAL = 2;
    static AmazonDynamoDB client ;
    static DynamoDB dynamoDB ;
    static Table table;
    static Table notificationTable;
    static {
		   client = AmazonDynamoDBClientBuilder.standard()
		            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("dynamodb.us-east-2.amazonaws.com", "us-east-2"))
		            .build();
         dynamoDB = new DynamoDB(client);
         table = dynamoDB.getTable("User_Posture");
         notificationTable = dynamoDB.getTable("Notification");
    }
	private static VoidFunction<JavaRDD<Posture>> func = new VoidFunction<JavaRDD<Posture>>() {

		private static final long serialVersionUID = -3376626873866221410L;

		@Override
		public void call(JavaRDD<Posture> t) throws Exception {
			// TODO Auto-generated method stub
			if(t.isEmpty()) {
				return;
			}
			Posture pMin = t.min(new PostureTimeComparator());
			
			Posture pMax = t.max(new PostureTimeComparator());
			Posture maxPosture = t.mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((i1,i2)-> i1+i2).max(new TupleComparator())._1();
			System.out.println(maxPosture.getPostureGrade());
			if("Very Poor".equals(maxPosture.getPostureGrade()) || "Poor".equals(maxPosture.getPostureGrade())) {
					sendNotification(maxPosture,pMin.getTime(),pMax.getTime());
			}
			sendPostureToDynamoDB(maxPosture,pMin.getTime(),pMax.getTime());
			System.out.println("Calling Callvbakc");
		}
	};
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamProcess").setMaster("local[4]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.minutes(INTERVAL));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "34.215.24.129:9092");

		Set<String> topics = new HashSet<>();
		topics.add("smartchair");
		
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				    ssc,
					String.class,
			        String.class,
			        StringDecoder.class,
			        StringDecoder.class,
			        kafkaParams,
			        topics );
		JavaDStream<String> lines = messages.map(Tuple2::_2);
		//JavaDStream<Object> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	    //JavaPairDStream<String, Integer> wordCounts = lines.mapToPair(s -> new Tuple2<>(s, 1))
	            //.reduceByKey((i1, i2) -> i1 + i2);
		JavaDStream<Posture> postures = lines.map(x -> getPosture(x));
		postures.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1,i2)->i1+i2).print();
		postures.foreachRDD(func);
		
		//numRecords.print();
		ssc.start();
		ssc.awaitTermination();
	}
	
	private static void sendMqtt(String title, String body) throws MqttException {
		MqttClient client = new MqttClient("tcp://test.mosquitto.org:1883", MqttClient.generateClientId());
		client.connect();
		MqttMessage message = new MqttMessage();
		String payload = new JSONObject().put("body", message).put("title", title).toString();
		message.setPayload(payload.getBytes());
		client.publish("notification_smartchair", message);
	}
	
    private static void sendPushNotification(String title, String message) throws Exception {
        /*
			String pushMessage = "{\"data\":{\"title\":\"" +
                title +
                "\",\"message\":\"" +
                message +
                "\"},\"to\":\"" +
                DEVICE_TOKEN +
                "\"}";
        */
        String pushMessage = 
        		//new JSONObject().put("data",
        		new JSONObject().put("message", 
        		new JSONObject()
        		.put("topic", "news")
        		.put("notification", new JSONObject().put("body", message)
        				.put("title", title)
        				)
        		//.put("token", DEVICE_TOKEN)
        				)
        		//)
        		.toString();
       System.out.println(pushMessage); 
       
       URL url = new URL("https://fcm.googleapis.com/v1/projects/smartchair-5e2da/messages:send");
       HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
       String accessToken= getAccessToken();
       System.out.println(accessToken);
       httpURLConnection.setRequestProperty("Authorization", "Bearer " + accessToken);
       httpURLConnection.setRequestProperty("Content-Type", "application/json; UTF-8");
       httpURLConnection.setDoOutput(true);
       httpURLConnection.setRequestMethod("POST");
        // Send FCM message content.
        OutputStream outputStream = httpURLConnection.getOutputStream();
        outputStream.write(pushMessage.getBytes());
        
        System.out.println(httpURLConnection.getResponseCode());
        System.out.println(httpURLConnection.getResponseMessage());
    }
    
    private static String getAccessToken() throws IOException {
    	  GoogleCredential googleCredential = GoogleCredential
    	      .fromStream(new FileInputStream("smartchair-5e2da-firebase-adminsdk-1bkgw-410f7f6556.json"))
    	      .createScoped(Collections.singleton("https://www.googleapis.com/auth/firebase.messaging"))
    	      ;
    	  googleCredential.refreshToken();
    	  return googleCredential.getAccessToken();
    	}
	
	private static void sendNotification(Posture posture,long startTime, long endTime) throws Exception {
		System.out.println("-- Sending Notification --");
		sendPushNotification("Correct your Posture", posture.getReco());
		sendMqtt("Correct your Posture", posture.getReco());
  		notificationTable.putItem(new Item().withString("NotificationId",UUID.randomUUID().toString()).withInt("UserID", posture.getUserId()).withLong("StartTime", startTime)
		.withLong("EndTime", endTime)
		.withString("PostureGrade", posture.getPostureGrade())
		.withString("PostureID", String.valueOf(posture.getPostureId()))
		.withString("Recommendation", posture.getReco()));
  		
	}
	
	private static void sendPostureToDynamoDB(Posture posture, long startTime, long endTime) {
		System.out.println(startTime + " --> " + endTime);
		System.out.println("Sending to DB");
  		table.putItem(new Item().withInt("UserID", posture.getUserId()).withLong("StartTime", startTime)
		.withLong("EndTime", endTime)
		.withString("PostureGrade", posture.getPostureGrade())
		.withString("PostureID", String.valueOf(posture.getPostureId()))
		.withString("Recommendation", posture.getReco()));
	}
	public static Posture getPosture(String sensorData) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		Sensor sensor = mapper.readValue(sensorData, Sensor.class);
		
		Posture posture = getActualPosture(sensor);
	
		//Posture posture = getRandomPosture();
		posture.setTime(sensor.getTime());
		
		return posture;
	}
	private static Posture getActualPosture(Sensor sensor) {
		int pBackRestTop = sensor.getPressure_one();
		int pBackRestBottom = sensor.getPressure_two();
		int pLowerBack = sensor.getPressure_three();
		int pEdgeOfSeat = sensor.getPressure_four();
		int distanceLower = sensor.getDistance_two();
		int distanceUpper = sensor.getDistance_one();
		int angle = sensor.getAngle();
		Posture posture = new Posture(); posture.setUserId(1);posture.setTime(sensor.getTime());
		// Not Sitting
		if(pLowerBack < 200 && pEdgeOfSeat < 200 && pBackRestBottom < 200 && pBackRestTop < 200 ) {
			posture.setReco("N/A");posture.setPostureId(8);posture.setPostureGrade("Not Sitting");
		}
		// recline too much
		else if(pLowerBack > 1000 && pEdgeOfSeat > 1000 && pBackRestBottom > 1000 && pBackRestTop > 1000 && angle > 120) {
			posture.setReco("Lean towards the front");posture.setPostureId(3);posture.setPostureGrade("Poor");
		}
		// Leaning front
		else if(pLowerBack > 1000 && pEdgeOfSeat > 1000 && pBackRestBottom > 1000 && pBackRestTop < 600 && distanceUpper > 30 && distanceLower < 10) {
			posture.setReco("Sit Upright");posture.setPostureId(5);posture.setPostureGrade("Very Poor");
		}
		//Lean back sitting at edge of seat
		else if(pEdgeOfSeat > 1000 && pBackRestTop > 1000 && pLowerBack < 600 && pBackRestBottom < 600) {
			posture.setReco("Sit Upright");posture.setPostureId(7);posture.setPostureGrade("Very Poor");
		}
		// Sitting at edge of seat
		else if(pEdgeOfSeat > 1000 && pBackRestTop < 600 && pLowerBack < 600 && pBackRestBottom < 600 ) {
			posture.setReco("Get into the chair more");posture.setPostureId(4);posture.setPostureGrade("Poor");
		}
		// sitting at center and leaning front
		else if(pEdgeOfSeat > 1000 && pBackRestTop < 600 && pLowerBack < 600 && pBackRestBottom < 600 && distanceUpper > 50 && distanceLower < 30 ) {
			posture.setReco("Get into the chair more");posture.setPostureId(6);posture.setPostureGrade("Poor");
		}
		else if(pLowerBack > 1000 && pEdgeOfSeat > 1000 && pBackRestBottom > 1000 && pBackRestTop > 1000) {
			posture.setReco("You're doing good");posture.setPostureId(2);posture.setPostureGrade("Very Good");
		}
		else {
			posture.setReco("You're doing good");posture.setPostureId(1);posture.setPostureGrade("Good");
		}
		return posture;
	}
	private static Posture getRandomPosture() {
		Posture p1 = new Posture(); p1.setReco("You're doing good");p1.setPostureId(1);p1.setPostureGrade("Good");
		Posture p2 = new Posture(); p2.setReco("You're doing good");p2.setPostureId(2);p2.setPostureGrade("Very Good");
		Posture p3 = new Posture(); p3.setReco("Lean towards the front");p3.setPostureId(3);p3.setPostureGrade("Poor");
		Posture p4 = new Posture(); p4.setReco("Lean towards the front");p4.setPostureId(4);p4.setPostureGrade("Poor");
		Posture p5 = new Posture(); p5.setReco("Sit Upright");p5.setPostureId(5);p5.setPostureGrade("Very Poor");
		Posture p6 = new Posture(); p6.setReco("Use the back rest");p6.setPostureId(6);p6.setPostureGrade("Poor");
		Posture p7 = new Posture(); p7.setReco("Sit Upright");p7.setPostureId(7);p7.setPostureGrade("Very Poor");
		Posture p8 = new Posture(); p8.setReco("N/A");p8.setPostureId(8);p8.setPostureGrade("Not Sitting");
		Posture[] p = {p1,p2,p3,p4,p5,p6,p7,p8};
		return p[(int)(Math.random()*8)];
	}
	
	
}
