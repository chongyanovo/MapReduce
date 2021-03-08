package advancetest2.mapreduce.task3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Bean implements WritableComparable<Bean> {
    private String shop;
    private String good;

    public String getShop() {
        return shop;
    }

    public void setShop(String shop) {
        this.shop = shop;
    }

    public String getGood() {
        return good;
    }

    public void setGood(String good) {
        this.good = good;
    }

    @Override
    public String toString() {
        return shop + "\t" + good;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(shop);
        dataOutput.writeUTF(good);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.shop = dataInput.readUTF();
        this.good = dataInput.readUTF();
    }

    @Override
    public int compareTo(Bean o) {
        int result = this.shop.compareTo(o.getShop());
        if (result == 0) {
            result = this.good.compareTo(o.getGood());
        }
        return result;
    }
}
