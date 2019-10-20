import java.io.*;

public class DataDivider {

    private static final String srcPath = "C:\\Users\\Disclover\\Desktop\\csdn_vectors.txt";
    private static final String trainSetPath = "C:\\Users\\Disclover\\Desktop\\csdn_train_vectors.txt";
    private static final String developmentSetPath = "C:\\Users\\Disclover\\Desktop\\csdn_development_vectors.txt";
    private static final String testSetPath = "C:\\Users\\Disclover\\Desktop\\csdn_test_vectors.txt";

    public static void main(String[] args) {
        File src = new File(srcPath);
        File trainSet = new File(trainSetPath);
        File developmentSet = new File(developmentSetPath);
        File testSet = new File(testSetPath);
        try {
            trainSet.createNewFile();
            developmentSet.createNewFile();
            testSet.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(src)));
            BufferedWriter trainWriter = new BufferedWriter(new FileWriter(trainSet));
            BufferedWriter developmentWriter = new BufferedWriter(new FileWriter(developmentSet));
            BufferedWriter testWriter = new BufferedWriter(new FileWriter(testSet));

            int index = 0;
            String line = null;
            while((line = reader.readLine()) != null){
                if (index < 15515){
                    trainWriter.write(line + "\n");
                    trainWriter.flush();
                }
                else if (index < 20687){
                    developmentWriter.write(line + "\n");
                    developmentWriter.flush();
                }
                else {
                    testWriter.write(line + "\n");
                    testWriter.flush();
                }
                index ++;
            }
            reader.close();
            trainWriter.close();
            developmentWriter.close();
            testWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
