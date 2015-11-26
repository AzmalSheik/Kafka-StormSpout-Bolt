import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DisplayBolt  extends BaseRichBolt
{
	 private static final long serialVersionUID = 3092938699134129356L;
	  
	  private OutputCollector collector;
	  
	  private Map<Long, Long> sensordata;
	  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector)
	  {
	    collector = outCollector;
	    
	  }
	  public void declareOutputFields(OutputFieldsDeclarer declarer) 
	  {
		  declarer.declare(new Fields("json_data"));
		  
	  }
	  
	  public void execute(Tuple tuple)
	  {
		  
		    Object value = tuple.getValue(0);
		    String rawtuple = null;
		    if (value instanceof String) {
		      rawtuple = (String) value;

		    } else {
		      // Kafkaspout returns bytes and converting it to string data
		      byte[] bytes = (byte[]) value;
		      try {
		    	  rawtuple = new String(bytes, "UTF-8");
		      } catch (UnsupportedEncodingException e) {
		        throw new RuntimeException(e);
		      }      
		    }

		   
		    collector.emit(new Values(rawtuple));
		    collector.ack(tuple);
		  }
	

}
