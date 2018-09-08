package cn.just.shinelon.local;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 实现简单的本地求和功能
 */
public class LocalSumTopology {


    /**
     * Spout组件
     * 产生数据并且发送
     */
    public static class SumSpout extends BaseRichSpout{

        private SpoutOutputCollector collector;

        /**
         * 初始化操作
         * @param conf 初始化配置项
         * @param context 上下文
         * @param collector 数据发射器
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector=collector;
        }

        int number=0;
        /**
         * 发射数据
         * 该方法是一个死循环
         */
        @Override
        public void nextTuple() {
            //Values类实现了ArrayList
            collector.emit(new Values(++number));

            System.out.println("Spout number: "+number);
            //防止数据产生太快，睡眠一秒
            Utils.sleep(1000);

        }

        /**
         * 定义输出端字段
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //与上面的number变量对应
            declarer.declare(new Fields("num"));
        }
    }

    /**
     * Bolt组件
     * 实现业务的逻辑处理，这里求和
     */
    public static class SumBolt extends BaseRichBolt{
        /**
         * 因为 这里接收数据之后不需要再发送给下一个Bolt，因此再初始化collector发射器
         * @param stormConf
         * @param context
         * @param collector
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        int sum=0;
        /**
         * 执行业务逻辑的处理
         * 该方法也是一个死循环
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            //可以通过字段名或者下标索引获取
            Integer value=input.getIntegerByField("num");
            sum+=value;
            System.out.println("Bolt sum: "+sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }


    public static void main(String[] args) {
        //使用TopologyBuilder设置Spout和Bolt，并且将其关联 在一起
        //创建Topology
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("SumSpout",new SumSpout());
        builder.setBolt("SumBolt",new SumBolt()).shuffleGrouping("SumSpout");
        //使用本地模式
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("LocalSumTopology",new Config(),builder.createTopology());
    }


}
