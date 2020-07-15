import java.util.Map;

public class App {

    public static void main(String[] args) {

        String path="D:\\Zone24x7\\Word-Count-Training\\Java\\src\\main\\resources\\word_count.txt";
        WordCount count=new WordCount();
        for (Map.Entry<String,Integer> entry : count.getWordCount(path).entrySet()) {
            System.out.println(entry.getKey()+"=>"+entry.getValue());

            }
        }
    }

