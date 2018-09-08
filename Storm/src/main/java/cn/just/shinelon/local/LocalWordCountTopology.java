package cn.just.shinelon.local;

import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * WordCount案例
 */
public class LocalWordCountTopology {

    /**
     * 从文件中读取数据，向bolt每次发送一行数据
     */
    public static class DataSourceSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
           this.collector=collector;
        }

        /**
         * 从文件中 读取数据
         */
        @Override
        public void nextTuple() {
            //listFiles方法传入一个目录，文件后缀名，是否递归读取三个参数
            Collection<File> fileList = FileUtils.listFiles(new File("G:\\aaa"),new String[]{"txt"},true);
            for(File file:fileList){
                try {
                    List<String> lineList=FileUtils.readLines(file);
                    for(String line:lineList){
                        this.collector.emit(new Values(line));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    //读取完一个文件之后，修改文件名，防止重复读取
                    FileUtils.moveFile(file,new File(file.getAbsolutePath()+System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 接收发送过来的每一行数据，按照逗号进行分割为单词
     */
    public static class SplitBolt extends BaseRichBolt{
        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        /**
         * 处理业务逻辑，按照逗号进行分割
         * @param input
         */
        @Override
        public void execute(Tuple input) {
           String line = input.getStringByField("line");
           String[] words = line.split(",");
           for (String word:words){
               collector.emit(new Values(word));
           }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
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
            String word=input.getStringByField("word");
            Integer count=wordCountMap.get(word);
            if(count==null){
               count=0;
            }
            count++;
            wordCountMap.put(word,count);

            System.out.println("==============================");
            //打印每次统计的结果
            for(Map.Entry<String,Integer> entry:wordCountMap.entrySet()){
                System.out.println(entry.getKey()+" : "+entry.getValue());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("LocalWordCountTopology",new Config(),builder.createTopology());
    }

}
