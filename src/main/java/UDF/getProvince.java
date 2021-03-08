package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

public class getProvince extends UDF {
    static HashMap<String, String> map = new HashMap<>();

    static {
        map.put("173", "江苏");
        map.put("174", "上海");
        map.put("175", "北京");
        map.put("176", "广州");
    }

    public static String evaluate(String ip) {
        String str = ip.substring(0, 3);
        return map.get(str);
    }

//    public static void main(String[] args) {
//        System.out.println(evaluate("17328278"));
//    }
}
