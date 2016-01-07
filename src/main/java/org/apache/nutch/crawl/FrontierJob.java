package org.apache.nutch.crawl;

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Link;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.storage.StorageUtils.createStore;
import static org.apache.nutch.storage.StorageUtils.getDataStoreClass;
import static org.apache.nutch.util.FilterUtils.getBatchIdLinkFilter;

/**
 * @author Pierre Sutra
 */
public class FrontierJob extends NutchTool {

  // Class fields
  public static final Logger LOG = LoggerFactory.getLogger(FrontierJob.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
  private static enum probes{
    NEW_PAGES
  }

  // Object fields
  private Configuration conf;

  public static class FrontierMapper
    extends GoraMapper<String, Link, String, Link> {

    private WebPage.Builder pageBuilder;
    private DataStore<String,WebPage> pageDB;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      try {
        pageDB = createStore(
          context.getConfiguration(), String.class, WebPage.class);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      pageBuilder = WebPage.newBuilder();
    }

    @Override
    public void map(String key, Link link, Context context)
      throws IOException, InterruptedException {

      WebPage outPage = pageBuilder.build();
      assert link.getOut()!=null;
      outPage.setKey(link.getOut());
      pageDB.putIfAbsent(link.getOut(), outPage);
      context.getCounter(probes.NEW_PAGES).increment(1);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      pageDB.close();
    }

  }

  public FrontierJob() {

  }

  public FrontierJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Map<String,Object> run(Map<String,Object> args) throws Exception {

    String batchId = (String) args.get(Nutch.ARG_BATCH);
    getConf().set(GeneratorJob.BATCH_ID, batchId);
    assert  batchId != null;

    LOG.info("FrontierJob: batchId:\t" + batchId);

    currentJob = new NutchJob(getConf(), "frontier");

    DataStore<String, Link> store = createStore(
      currentJob.getConfiguration(),
      String.class, Link.class);

    if (store == null)
      throw new RuntimeException("Could not create datastore");

    Query<String, Link> query = store.newQuery();
    if (!batchId.equals("-all"))
      query.setFilter(getBatchIdLinkFilter(batchId));
    query.setSortingOrder(true);

    GoraMapper.initMapperJob(
      currentJob,
      query,
      store,
      String.class,
      Link.class,
      FrontierMapper.class,
      null,
      false);

    GoraOutputFormat.setOutput(
      currentJob,
      getDataStoreClass(currentJob.getConfiguration()),
      String.class,
      Link.class,
      true);

    currentJob.setReducerClass(Reducer.class);
    currentJob.setNumReduceTasks(0);

    currentJob.waitForCompletion(true);
    LOG.info("FrontierJob: new page(s): "+ currentJob.getCounters().findCounter(probes.NEW_PAGES).getValue());
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  public int frontier(String batchId) throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.debug("FrontierJob: starting at " + sdf.format(start));

    run(ToolUtil.toArgMap(
      Nutch.ARG_BATCH, batchId
    ));

    long finish = System.currentTimeMillis();
    LOG.info("FrontierJob: finished at " + sdf.format(finish) + ", time elapsed: " + TimingUtil.elapsedTime(start, finish));
    return 0;
  }


  public int run(String[] args) throws Exception {

    String batchId = null;

    if (args.length < 1) {
      System.err.println("Usage: FrontierJob <batchId> [-crawlId <id>]");
      System.err.println("    <batchId>     - symbolic batch ID created by Generator or -all");
      System.err.println("    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)");
      return -1;
    }

    for (int i = 0; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      } else {
        if (batchId != null) {
          System.err.println("BatchId already set to '" + batchId + "'!");
          return -1;
        }
        batchId = args[i];
      }
    }

    return frontier(batchId);

  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(
      NutchConfiguration.create(),
      new FrontierJob(),
      args);
    System.exit(res);
  }

}
