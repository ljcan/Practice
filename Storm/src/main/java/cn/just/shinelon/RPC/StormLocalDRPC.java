package cn.just.shinelon.RPC;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class StormLocalDRPC {

    public static class DRPCBolt extends BaseRichBolt{

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            Object requestId = input.getValue(0);
            String value = input.getString(1);
            String result = "add user "+value;

            System.out.println(result);

            collector.emit(new Values(requestId,result));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("requestId","result"));
        }
    }

    public static void main(String[] args) {
        //参数为方法名
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
        builder.addBolt(new DRPCBolt());

        LocalDRPC drpc = new LocalDRPC();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormLocalDRPC",new Config(),builder.createLocalTopology(drpc));
        //第一个参数为调用的方法名，第二个参数为方法参数
        String result=drpc.execute("addUser","shinelon");
        System.err.println("From Client "+result);

        cluster.shutdown();
        drpc.shutdown();
    }
}
