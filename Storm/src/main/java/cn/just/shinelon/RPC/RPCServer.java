package cn.just.shinelon.RPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCServer {


    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        RPC.Server server = new RPC.Builder(configuration)
                            .setProtocol(UserService.class)
                            .setInstance(new UserServiceImpl())
                            .setBindAddress("localhost")
                            .setPort(8888)
                            .build();

        server.start();


    }
}
