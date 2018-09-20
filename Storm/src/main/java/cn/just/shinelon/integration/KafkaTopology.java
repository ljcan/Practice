package cn.just.shinelon.integration;

import cn.just.shinelon.utils.DateUtil;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

public class KafkaTopology {


    /**
     * 源码：
     * public class RawMultiScheme implements MultiScheme {
     public RawMultiScheme() {
     }

     public Iterable<List<Object>> deserialize(ByteBuffer ser) {
     return Arrays.asList(Utils.tuple(new Object[]{Utils.toByteArray(ser)}));
     }

     public Fields getOutputFields() {
     return new Fields(new String[]{"bytes"});
     }
     }
     */
    public static class PrintBolt extends BaseRichBolt{
        private OutputCollector outputCollector;


        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector=outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            try {
            byte[] bytes=tuple.getBinaryByField("bytes");
            String input = new String(bytes);
            String[] logs = input.split("\t");
            String phone = logs[0];
            String tmp = logs[1];
            Double longitude = Double.parseDouble(tmp.split(",")[0]);
            Double latitude = Double.parseDouble(tmp.split(",")[1]);
            long timestamp = DateUtil.getInstance().getTime(logs[2]);
            System.out.println(phone+", "+longitude+","+latitude+", "+timestamp);

            outputCollector.emit(new Values(timestamp,latitude,longitude));

            outputCollector.ack(tuple);

            } catch (Exception e) {
                e.printStackTrace();
                outputCollector.fail(tuple);
            }



        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields("time","latitude","longitude"));
        }
    }






    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider;
        connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);


        String tableName = "location";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into location values(?,?,?)")
                .withQueryTimeoutSecs(30);

        BrokerHosts hosts = new ZkHosts("hadoop-senior.shinelon.com:2181");
        String topicName="storm_project";
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());

        spoutConfig.startOffsetTime=kafka.api.OffsetRequest.LatestTime();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("KafkaSpout",kafkaSpout);
        builder.setBolt("PrintBolt",new PrintBolt()).shuffleGrouping("KafkaSpout");
        builder.setBolt("JdbcInsertBolt",userPersistanceBolt).shuffleGrouping("PrintBolt");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("KafkaTopology",new Config(),builder.createTopology());



    }



}
