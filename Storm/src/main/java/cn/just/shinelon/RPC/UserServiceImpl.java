package cn.just.shinelon.RPC;

public class UserServiceImpl implements UserService{
    @Override
    public void insertUser(String name, int age) {
        System.out.println("name :"+name+", age :"+age);
    }
}
