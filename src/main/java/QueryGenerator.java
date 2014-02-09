import indexer.WordsGenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class QueryGenerator {

    private final static String QUERY_TIME_JOIN = "q={!join from=parentId to=id}search_field:%s&fl=id,data_source&sort=score desc";
    private final static String BLOCK_JOIN = "q={!parent which=type:parent}search_field:%s&fl=id,data_source&sort=score desc";
    private final static String FIELD_COLLAPSING = "q=search_field:%s&fq={!collapse field=parentId}&fl=id,data_source&sort=score desc";
    private final static int QUERIES_COUNT = 10000;

    public static void main(String[] str) {
        try {
            WordsGenerator.init();
            BufferedWriter queryJoin = new BufferedWriter(new FileWriter(new File("queries-join.txt")));
            BufferedWriter blockJoin = new BufferedWriter(new FileWriter(new File("block-join.txt")));
            BufferedWriter fieldCollapsing = new BufferedWriter(new FileWriter(new File("field-collapsing.txt")));
            for (int i = 0; i < QUERIES_COUNT; i++) {
                String term = getTerms();
                addTerm(queryJoin, QUERY_TIME_JOIN, term);
                addTerm(blockJoin, BLOCK_JOIN, term);
                addTerm(fieldCollapsing, FIELD_COLLAPSING, term);
            }
            queryJoin.close();
            blockJoin.close();
            fieldCollapsing.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addTerm(BufferedWriter bufferedWriter, String pattern, String term) throws IOException {
        bufferedWriter.write(String.format(pattern,term));
        bufferedWriter.write("\n");

    }
    static String getTerms() {
        return WordsGenerator.getCommonWord();
    }
}
