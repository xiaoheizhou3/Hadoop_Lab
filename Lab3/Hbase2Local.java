package cn.edu.nju;
import java.io.FileWriter;
import java.io.IOException;
//import java.io.FileWriter;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.PrintWriter;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;


public class Hbase2Local {
    static Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("deprecation")
	public static void getResultScan(String tableName, String filePath) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table =  new HTable(conf, tableName);
        try {
            rs = table.getScanner(scan);
            FileWriter fos = new FileWriter(filePath);
            for (Result r : rs) {
                for (KeyValue kv : r.raw()) {
                    String s = new String(new String(kv.getRow()) + "\t" + new String(kv.getValue()) + "\n");
                    fos.write(s);
                }
            }
            fos.close();
        } catch (IOException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        rs.close();
    }
    public static void main(String[] args) throws Exception {
        String tableName = "Wuxia";
        String filePath = "/home/xiaohei/Desktop/Wuxia.txt";
        getResultScan(tableName, filePath);
    }
}