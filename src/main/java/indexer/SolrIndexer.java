package indexer;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SolrIndexer {
    protected final int NUM_OF_MASTER_DOCS = 4000000;
    protected final int CHILDREN_COUNT = 6;

    private final int NUM_WORDS_IN_TEXT_DOC = 30;

    private final int RATIO_COMMON_WORDS = 7;
    private final int RATIO_ENGLISH_WORDS = 3;

    protected final int NUM_DOCS_IN_PACKET = 300;
    protected final int COMMIT_WITHIN = 60000;


    private final String SOLR_SERVER_URL = "http://localhost:8983/solr";

    int _counter = 26104000; // Just a counter to insure doc IDs are unique.

    private int docNum = 0;


    protected ConcurrentUpdateSolrServer server;
    protected List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();

    public SolrIndexer() throws Exception {
        server = new ConcurrentUpdateSolrServer(SOLR_SERVER_URL, 4, 2);
        server.setParser(new XMLResponseParser());
    }

    void generateAllDocs() throws Exception {
        generateDocs();
        if (docList.size() > 0) {
            addDocs();
            docList.clear();
        }
        server.commit();
    }

    void generateDocs() throws Exception {
        long timeStart = System.currentTimeMillis();
        for (int idx = 0; idx < NUM_OF_MASTER_DOCS; ++idx) {
            SolrInputDocument parentDoc = generateParentDoc();
            docList.add(parentDoc);
            String id = parentDoc.get("id").getValue().toString();
            String dataSource = parentDoc.get("data_source").getValue().toString();
            for (int i = 0; i < CHILDREN_COUNT; i++) {
                addChild(parentDoc, generateChildrenDoc(id, dataSource));
            }
            if (docList.size() >= NUM_DOCS_IN_PACKET) {
                server.add(docList, COMMIT_WITHIN);
                docNum += docList.size();
                log("Send docs count " + docNum + " All counts is " +  NUM_OF_MASTER_DOCS+" Time in sec from start "+
                        (System.currentTimeMillis() - timeStart)/1000);
                docList.clear();
            }
        }
    }

    static void log(String msg) {
        System.out.println(msg);
    }


    protected void addChild(SolrInputDocument parent, SolrInputDocument solrInputDoc) {
        docList.add(solrInputDoc);
    }

    protected void addDocs() throws SolrServerException, IOException {
        server.add(docList);
    }

    SolrInputDocument generateChildrenDoc(String parentId, String parentDataSource) throws Exception {
        StringBuilder builder = new StringBuilder();
        int wordCount = 0;
        while (wordCount < NUM_WORDS_IN_TEXT_DOC) {
            for (int jdx = 0; jdx < RATIO_COMMON_WORDS; ++jdx) {
                WordsGenerator.getCommonWord(builder);
                ++wordCount;
            }
            for (int jdx = 0; jdx < RATIO_ENGLISH_WORDS; ++jdx) {
                WordsGenerator.getEnglishWord(builder);
                ++wordCount;
            }
        }

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", generateId());
        doc.addField("parentId", parentId);
        doc.addField("data_source", parentDataSource);
        doc.addField("search_field", builder.toString());
        return doc;
    }

    SolrInputDocument generateParentDoc() throws Exception {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", generateId());
        doc.addField("data_source", WordsGenerator.getCommonWord());
        doc.addField("type", "parent");
        return doc;

    }

    private String generateId() {
        return WordsGenerator.getCommonWord() + "_" + _counter++;
    }
}
