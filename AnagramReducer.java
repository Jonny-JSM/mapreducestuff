import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {
    public ArrayList<String> anagramList = new ArrayList<>();
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String anagram = null;
        for (Text val : values) {
                if (anagram == null) {
                   anagram = val.toString();
                } else {
                    anagram = anagram + ", " + val.toString();
                }
        }
        // create an array from the anagram set values
        String[] anagramArr = anagram.split(", ");
        Arrays.sort(anagramArr);
        String uniqueAnagramStrings = anagramArr[0];

        for (String anaArrValue : anagramArr) {
            if ( uniqueAnagramStrings.indexOf(anaArrValue) == -1  ) {
                uniqueAnagramStrings = uniqueAnagramStrings + ", " + anaArrValue;
            }
        }

        String[] uniqueAnagramArray = uniqueAnagramStrings.split(", ");
        ArrayList<String> uniqueCountList = new ArrayList<>();

        if (uniqueAnagramArray.length >= 2) {
            for (String uniqueAna : uniqueAnagramArray) {
                int repeatCount = 0;
                for (int i = 0; i < anagramArr.length; i++) {
                    if (anagramArr[i].contains(uniqueAna)) {
                        repeatCount++;
                    }
                }
                uniqueCountList.add(uniqueAna + " : " +repeatCount+ " repetitions");
            }
            anagramList.add("Unique Anagram Count: "+uniqueAnagramArray.length+", "+""+ uniqueCountList.toString().replaceAll("\\[","")
                    .replaceAll("\\]",""));
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        Collections.sort(anagramList);
        for(String a : anagramList){
            context.write(new Text(a.split(", ")[0]), new Text(Arrays.toString(Arrays.copyOfRange(a.split(", " ),1,a.split(", ").length))));
        }
    }
}