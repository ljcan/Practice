package cn.just.shinelon.integration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HBaseWordCountTopology {


    public static class DataSourceSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
           this.collector=collector;
        }

        String[] words={"shinelon","tom","jack","mary"};

        @Override
        public void nextTuple() {
            Random random = new Random();
            String word = words[random.nextInt(words.length)];
            collector.emit(new Values(word));

            System.out.println("emit word "+word);

            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            declarer.declare(new Fields("line"));
        }
    }

    public static class SplitBolt extends BaseRichBolt{
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }


        @Override
        public void execute(Tuple input) {
           String word = input.getStringByField("line");
           collector.emit(new Values(word));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            declarer.declare(new Fields("words"));
        }
    }

    public static class CountBolt extends BaseRichBolt{
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        Map<String,Integer> wordCountMap=new HashMap<String,Integer>();
        /**
         * 完成单词的计数功能
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            String word=input.getStringByField("words");
            Integer count=wordCountMap.get(word);
            if(count==null){
               count=0;
            }
            count++;
            wordCountMap.put(word,count);
            collector.emit(new Values(word,wordCountMap.get(word)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
    }


    public static void main(String[] args) {


        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        Config config = new Config();
        Map<String,Object> configMap = new HashMap<String,Object>();
        configMap.put("hbase.rootdir","hdfs://hadoop-senior.shinelon.com:8020/hbase");
        configMap.put("hbase.cluster.distributed","true");
        configMap.put("hbase.zookeeper.quorum","hadoop-senior.shinelon.com");
        config.put("hbase.conf",configMap);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")    //rowkey
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");

        HBaseBolt hbaseBolt = new HBaseBolt("wc", mapper)
                .withConfigKey("hbase.conf");

        builder.setBolt("HBaseBolt",hbaseBolt).shuffleGrouping("CountBolt");

        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("HBaseWordCountTopology",config,builder.createTopology());
    }

}
