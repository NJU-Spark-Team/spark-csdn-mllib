import com.alibaba.fastjson.JSON;
import org.bson.Document;

import java.io.*;

public class DataReader {

    private static final String path = "C:\\Users\\Disclover\\Desktop\\Detail\\Data\\cloud.json";

    public static void main(String[] args) {
        MongoUtil mongoUtil = new MongoUtil("SparkMLlib");
        File file = new File(path);
        String encoding = "UTF-8";
        long start = System.currentTimeMillis();
        try {
            InputStreamReader in = new InputStreamReader(new FileInputStream(file), encoding);
            BufferedReader reader = new BufferedReader(in);
            String line = null;
            int cnt = 0;
            while ((line = reader.readLine()) != null){
                Record record = JSON.parseObject(line, Record.class);
                Document document = record.toDoc();
                mongoUtil.insert("csdn_data", document);
                cnt ++;
            }
            System.out.println("Total records: " + cnt);
            System.out.println("Total time: " + (System.currentTimeMillis() - start) + " ms");
            reader.close();
            in.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class Record{
    public String title;
    public String url;
    public String summary;
    public String views;
    public String article;

    public Document toDoc(){
        Document document = new Document()
                .append("title", title)
                .append("url", url)
                .append("summary", summary)
                .append("views", views)
                .append("article", article);
        return document;
    }
}
