package cn.just.shinelon.RPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class RPCClient {


    public static void main(String[] args) throws Exception{
        //必须对应服务端接口的versionID
        long clientVersion=666;
        Configuration configuration = new Configuration();
        UserService userService = RPC.getProxy(UserService.class,clientVersion,
                new InetSocketAddress("localhost",8888)
                ,configuration);

        userService.insertUser("shinelon",19);

        RPC.stopProxy(userService);
    }
}
