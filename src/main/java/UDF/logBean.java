package UDF;

public class logBean {
    //Date,ip,账号,url,响应时间,访问结果
    private String data;
    private String ip;
    private String id;
    private String url;
    private String time;
    private String result;

    @Override
    public String toString() {
        return data + "\t" + ip + "\t" + id + "\t" +
                url + "\t" + time + "\t" + result ;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
