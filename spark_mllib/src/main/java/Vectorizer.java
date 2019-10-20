import org.bson.Document;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Vectorizer {

    private static final String path = "C:\\Users\\Disclover\\Desktop\\csdn_vectors.txt";

    public static void main(String[] args) {
        File file = new File(path);
        long start = System.currentTimeMillis();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            MongoUtil mongoUtil = new MongoUtil("SparkMLlib");
            Document document = null;

            while ((document = mongoUtil.selectNext("csdn_data")) != null){
                int[] vec = new int[200];
                String content = document.getString("article");
                for (int i = 0; i < content.length(); i ++){
                    char c = content.charAt(i);
                    int hash = String.valueOf(c).hashCode();
                    int index = hash % 200;
                    vec[index] ++;
                }
                StringBuffer stringBuffer = new StringBuffer();
                for (int i : vec){
                    stringBuffer.append("," + i);
                }
                String vecStr = stringBuffer.toString().substring(1);
//            Document vecDoc = new Document()
//                    .append("vector", vecStr);
//            mongoUtil.insert("csdn_vector", vecDoc);
                writer.write(vecStr + "\n");
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("Total time: " + (System.currentTimeMillis() - start));
    }
}
