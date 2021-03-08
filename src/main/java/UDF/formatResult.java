package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class formatResult extends UDF {
    public String evaluate(String result) {
        return result.toUpperCase();
    }
}
