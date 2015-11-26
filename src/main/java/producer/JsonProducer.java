package producer;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

import org.json.JSONObject;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class JsonProducer 
{
	 
	 private static int tempsensor_ID;
	 private static int temp;
	 private static int pressuresensor_ID;
	 private static int pressure;
	 private static int humiditysensor_ID;
	 private static int humidity;
	
	 private static Random rnd=new Random();
	 private static String generateJsonhumidity()
	 {
		 JSONObject tuple2 = new JSONObject();
		 humiditysensor_ID= rnd.nextInt(100);
	    	humidity=rnd.nextInt(1000);
	    	tuple2.put("humiditysensor_ID", humiditysensor_ID);
	    	tuple2.put("humidity", humidity);
	       return tuple2.toString();
	 }
	 private static String generateJsonpressure()
	 {
		 JSONObject tuple1 = new JSONObject();
	    	pressuresensor_ID= rnd.nextInt(100);
	    	pressure=rnd.nextInt(1000);
	    	tuple1.put("pressuresensor_ID", pressuresensor_ID);
	    	tuple1.put("pressure", pressure);
	       return tuple1.toString();
	 }
	   private static String generateJsontemp()
	   {
		      	JSONObject tuple = new JSONObject();
		    	tempsensor_ID= rnd.nextInt(100);
		    	temp=rnd.nextInt(1000);
		    	tuple.put("tempsensor_ID", tempsensor_ID);
		    	tuple.put("temp", temp);
		       return tuple.toString();
	   }
	public static void main(String[] args) 
	{
		 int count=1;
	        Properties props = new Properties();
	        props.put("metadata.broker.list", "ems.winext.com:9092");
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        props.put("partitioner.class", "main.java.SimplePartitioner");
	        props.put("request.required.acks", "1");

	        ProducerConfig config = new ProducerConfig(props);

	        Producer<String, String> producer = new Producer<String, String>(config);

	    for(;;)
	    {
	        if(count<=3)
	        {
	        switch(count)
	        {
	        case 1:
	        {
	            KeyedMessage<String, String> data = new KeyedMessage<String, String>("sensor-data",generateJsontemp());
	            producer.send(data);
	            count++;
	            break;
	        }
	        case 2:
	        {
	        KeyedMessage<String, String> data = new KeyedMessage<String, String>("sensor-data",generateJsonpressure());
	            producer.send(data);
	            count++;
	            break;
	        }
	        case 3:
	        {
	                KeyedMessage<String, String> data = new KeyedMessage<String, String>("sensor-data",generateJsonhumidity());
	            producer.send(data);
	            count++;
	            break;
	        }

	        }
	        }else
	        {
	                count=1;
	                continue;
	        }
	    }

	}

}
