package cn.just.shinelon.RPC;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

public class StormRemoteDRPCClient {

    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.put("storm.thrift.transport","org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES,3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL,10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING,20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE,104576);

        DRPCClient client = new DRPCClient(config,"hadoop-senior.shinelon.com",3772);
        String result = client.execute("addUser","shinelon");

        System.out.println("From Server "+result);

    }
}
