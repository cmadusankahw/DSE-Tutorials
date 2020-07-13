package counters;

import records.Record;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class OccurencesOfWords {

    private final Record record;

    public OccurencesOfWords(Record record) {
        this.record = record;
    }

    public Map<String, Integer> wordCount() {

        ArrayList <String>wordList=new ArrayList();

        try {


            BufferedReader bufferedReader = record.getReader();
            String line = bufferedReader.readLine();

            while (line != null) {
                String[] words = line.split(" ");
                for (String word : words) {

//                   Remove Invalid characters and Numbers
                    word=word.replaceAll("[,()\"\"]","").replaceAll("\\[\\d+\\]","").toLowerCase();
                    wordList.add(word);
                }
                line = bufferedReader.readLine();
            }

        }catch (IOException e){

        }

        Map<String, Integer> map = new HashMap<String, Integer>();
        for (String s:wordList) {

            if (!map.containsKey(s)) {  // first time we've seen this string
                map.put(s, 1);
            }
            else {
                int count = map.get(s);
                map.put(s, count + 1);
            }
        }
        return map;
    }
}
