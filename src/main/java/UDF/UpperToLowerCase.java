package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UpperToLowerCase extends UDF {
    /*
     * 重载evaluate
     * 访问限制必须是public
     */
    public String evaluate(String word) {
        String lowerWord = word.toLowerCase();
        return lowerWord;
    }
}
