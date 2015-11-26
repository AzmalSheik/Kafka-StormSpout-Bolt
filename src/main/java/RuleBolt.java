import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class RuleBolt extends BaseRichBolt 
{
	private static long tempsensor_ID;
	 private static long temp;
	 private static long pressuresensor_ID;
	 private static long pressure;
	 private static long humiditysensor_ID;
	 private static long humidity;
	 
	 
	 String emailid ;
	 
	 public RuleBolt(String emaildId){
		 emailid = emaildId;
	 }
	  private OutputCollector collector;
	 
	  public void triggerMail(String message,String emailID)
	  {
		  Properties emailProperties;
			Session mailSession;
			MimeMessage emailMessage;
			try
			{
		  String emailPort = "587";//gmail's smtp port

			emailProperties = System.getProperties();
			emailProperties.put("mail.smtp.port", emailPort);
			emailProperties.put("mail.smtp.auth", "true");
			emailProperties.put("mail.smtp.starttls.enable", "true");
			String toEmails = emailID;
			String emailSubject = "Alert Email";
			String emailBody = message;

			mailSession = Session.getDefaultInstance(emailProperties, null);
			emailMessage = new MimeMessage(mailSession);

			
			emailMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmails));
			

			emailMessage.setSubject(emailSubject);
			emailMessage.setContent(emailBody, "text/html");
			String emailHost = "smtp.gmail.com";
			String fromUser = "winboldtest@gmail.com";
			String fromUserEmailPassword = "winbold123";

			Transport transport = mailSession.getTransport("smtp");

			transport.connect(emailHost, fromUser, fromUserEmailPassword);
			transport.sendMessage(emailMessage, emailMessage.getAllRecipients());
			transport.close();
			
			}catch(Exception e)
			{
				e.printStackTrace();
			}
			
		  
	  }

	    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) 
	    {
	        this.collector = collector;
	    }

	    public void execute( Tuple tuple ) 
	    { 
	    	String jsonraw=tuple.getString(0);
	    	//creating json object for rule for different entries i.e, temperature,pressure ,humidity
	    	JSONObject json_data = (JSONObject)JSONValue.parse(jsonraw);
	    	JSONObject temprule=new JSONObject();
	    	temprule.put("tempsensor_ID",new Long(66));
	    	temprule.put("temp",new Long(990));
	    	JSONObject presrule=new JSONObject();
	    	presrule.put("pressuresensor_ID",new Long(87));
	    	presrule.put("pressure",new Long(990));
	    	JSONObject humidityrule=new JSONObject();
	    	humidityrule.put("humiditysensor_ID",new Long(21));
	    	humidityrule.put("humidity",new Long(30));
	    	
	    	//checking device id wrt rule data
	    	if(json_data.containsKey("tempsensor_ID"))
	    	{
	    		long tempid=Long.parseLong(json_data.get("tempsensor_ID").toString());
			    long temperature=Long.parseLong(json_data.get("temp").toString());
	    		if(tempid==(Long.parseLong(temprule.get("tempsensor_ID").toString())))
	  	      {
	  	    	 if(temperature>=(Long.parseLong(temprule.get("temp").toString())))
	  	    	  {
	  	    		System.out.println("json data: "+json_data);
	  	    		 
	  	    		  triggerMail("Temperature is above your mentioned limit",emailid);
	  	    		  System.out.println("done temperature mail");
	  	    	  }
	  	      }
	    	}
	    	
	    	
	    	if(json_data.containsKey("pressuresensor_ID"))
	    	{
	    		long presid =Long.parseLong((json_data.get("pressuresensor_ID")).toString());
	    		long pres =Long.parseLong((json_data.get("pressure")).toString());
	    			if(presid==(Long.parseLong((presrule.get("pressuresensor_ID")).toString())))
		  	      {
		  	    	 
		  	    	  if(pres >=(Long.parseLong((presrule.get("pressure")).toString())))
		  	    	  {
		  	    		System.out.println("json data: "+json_data);
		  	    		  
		  	    		  triggerMail("Pressure is above your mentioned limit",emailid);
		  	    		  System.out.println("done pressure mail");
		  	    	  }
		  	      }
	    	}
	    	
	    			
	    	if(json_data.containsKey("humiditysensor_ID"))
	    	{
	    		
	    		long humidityid =Long.parseLong((json_data.get("humiditysensor_ID")).toString());
	    		long humidity =Long.parseLong((json_data.get("humidity")).toString());
	    		
	    		
	    		
	    		if(humidityid==(Long.parseLong((humidityrule.get("humiditysensor_ID")).toString())))
		  	      {
	    			
		  	    	  if(humidity<=(Long.parseLong((humidityrule.get("humidity")).toString())))
		  	    	  {
		  	    		System.out.println("json data: "+json_data);
		  	    		  
		  	    		  triggerMail("Humidity is going below ",emailid);
		  	    		System.out.println("done location mail");
		  	    	  }
		  	      }
	    	}
	    	
	    	
	    }

	    public void declareOutputFields( OutputFieldsDeclarer declarer ) 
	    {
	        declarer.declare( new Fields( "jsondata" ) );
	    }   


}
