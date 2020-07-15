import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WordCount {


    public Map<String,Integer> getWordCount(String filePath){

        BufferedReader reader=null;

        try{

        FileReader r=new FileReader(filePath);
        reader = new BufferedReader(r);

    }catch (FileNotFoundException e){
            System.out.println("Given File is not valid");
        }

        Map<String, Integer> map = new HashMap<String, Integer>();

        try {
            String line=reader.readLine();
            while (line!=null){
                line=line.replaceAll("[^A-Za-z]+"," ").replaceAll("\n", " " ).toLowerCase();
                String[]words=line.split(" ");

                for(String word:words){

                    if (!map.containsKey(word)) {
                        map.put(word, 1);
                    }
                    else {
                        int count = map.get(word);
                        map.put(word, count + 1);
                    }
                }

                line=reader.readLine();
            }
        }catch (IOException e){

        }

        return map;

    }
}
