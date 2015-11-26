import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class JsonSampleTopology {

	private static KafkaSpout buildKafkaSentenceSpout() {
	    String zkHostPort = "ems.winext.com:2181";
	    String topic = "sensor-data";

	    String zkRoot = "/kafka-json";
	    String zkSpoutId = "kafka json spout";
	    ZkHosts zkHosts = new ZkHosts(zkHostPort);
	    
	    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
	    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
	    return kafkaSpout;
	  }
	public static void main(String[] args) throws Exception
	{	
		String arg1 = args[1];
		int numSpoutExecutors = 1;
		 KafkaSpout kspout = buildKafkaSentenceSpout();
		 System.out.println("kspout: "+kspout);
		 TopologyBuilder builder = new TopologyBuilder();
		    
		    builder.setSpout("kafka-json-spout", kspout, numSpoutExecutors);
		    builder.setBolt("parsebolt", new DisplayBolt()).shuffleGrouping("kafka-json-spout");
		    builder.setBolt("RuleBolt", new RuleBolt(arg1)).shuffleGrouping("parsebolt");
		    
		    Config cfg = new Config();    
		    StormSubmitter.submitTopology(args[0], cfg, builder.createTopology());

	}


}
