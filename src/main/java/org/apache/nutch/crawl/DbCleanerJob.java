package org.apache.nutch.crawl;

import org.apache.gora.store.DataStore;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Pierre Sutra
 */
public class DbCleanerJob extends NutchTool {

  @Override
  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    DataStore<String, WebPage> store = StorageUtils.createStore(getConf(), String.class, WebPage.class);
    store.deleteSchema();
    return null;
  }

  @Override
  public int run(String[] strings) throws Exception {
    run(new HashMap<String, Object>());
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
      NutchConfiguration.create(), 
      new DbCleanerJob(),
      args);
    System.exit(res);
  }
  
}
