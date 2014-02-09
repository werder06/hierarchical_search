package indexer;


import org.apache.solr.common.SolrInputDocument;

public class BJQIndexer extends SolrIndexer {

  public BJQIndexer() throws Exception {
    super();
  }

  @Override
  protected void addChild(SolrInputDocument doc, SolrInputDocument child) {
     doc.addChildDocument(child);
  }
  public static void main(String[] args) throws Exception {
    WordsGenerator.init();
    BJQIndexer d = new BJQIndexer();
    d.generateAllDocs();
  }
}
