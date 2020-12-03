import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;

class AnagramMapper extends Mapper<Object, Text, Text, Text> {
    URL url;
    String result = "";
    {
        try {
            url = new URL("https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt");
            Scanner scan = new Scanner(url.openStream());
            StringBuffer stringBuff = new StringBuffer();
            while(scan.hasNext()) {
                stringBuff.append(scan.next());
            }
            result = stringBuff.toString();
            System.out.println(result);
            result = result.replaceAll("<[^>]*>", "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer iterable = new StringTokenizer(value.toString());

        String stopWord = Arrays.toString(result.split((", ")));

        while (iterable.hasMoreTokens()) {
            String word = iterable.nextToken();
            word = Arrays.toString(word.replaceAll("[\\p{Punct}&&[^']]+", "").toLowerCase().split("\\s+"));
            word = word.replaceAll("\\[","").replaceAll("\\]","");
            char[] arr = word.toCharArray();
            Arrays.sort(arr);
            String wordKey = new String(arr);
            if (!stopWord.contains(word)) {
                context.write(new Text(wordKey), new Text(word));
            }
        }
    }
}
