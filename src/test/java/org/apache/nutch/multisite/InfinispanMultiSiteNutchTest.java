package org.apache.nutch.multisite;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.gora.GoraTestDriver;
import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.KeyWebPage;
import org.apache.nutch.scoring.opic.TestOPICScoringFilter;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ServerTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.Future;

import static org.apache.nutch.fetcher.TestFetcher.addUrl;
import static org.apache.nutch.util.ServerTestUtils.createPages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author PIerre Sutra
 */
public class InfinispanMultiSiteNutchTest extends AbstractMultiNutchSiteTest {

  public static final Logger LOG
    = LoggerFactory.getLogger(InfinispanMultiSiteNutchTest.class);

  @Override
  protected GoraTestDriver createDriver() {
    List<String> cacheNames = new ArrayList<>();
    cacheNames.add(WebPage.class.getSimpleName());
    return new GoraInfinispanTestDriver(numberOfSites(),numberOfNodes(),cacheNames);
  }

  @Override
  protected int numberOfSites() {
    return 1;
  }

  @Override
  protected int numberOfNodes() {
    return 3;
  }

  @Override
  protected int splitSize() {
    return 100;
  }

  @Override
  protected String connectionString(int i) {

    String ret="";

    String connectionString = ((GoraInfinispanTestDriver) driver).connectionString();
    String[] splits = connectionString.split("\\|");

    if (splits.length==1)
      return connectionString;

    for(int j=0; j<splits.length; j++){
      String split = splits[j];
      if (j!=i)
        ret+=split+"|";
    }

    return splits[i]+"|"+ret.substring(0, ret.length() - 1);
  }

  @Before
  @Override
  public void setUpClass() throws Exception {
    super.setUpClass();
  }

  @After
  @Override
  public void tearDownClass() throws Exception {
    super.tearDownClass();
  }

  @Test
  public void inject() throws Exception {

    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < nbPages(); i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }

    long start = System.currentTimeMillis();
    site(0).inject(urls).get();
    LOG.info("Inject speed: "+(nbPages()/((System.currentTimeMillis()-start)*Math.pow(10,-3)))+" page/s");
    assertEquals(nbPages(), readPageDB(null).size());

  }

  @Test
  public void generate() throws Exception {
    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < nbPages(); i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=0");
    }
    
    // inject
    site(0).inject(urls).get();

    List<Future<String>> futures = new ArrayList<>();

    // generate
    for (NutchSite site : sites)
      futures.add(site
        .generate(nbPages(), System.currentTimeMillis(), false, false));

    for (Future<String> future : futures)
      future.get();


    // check result
    assertEquals(nbPages(),readPageDB(Mark.GENERATE_MARK).size());

  }

  @Test
  public void fetch() throws Exception {

    Configuration conf = NutchConfiguration.create();

    // start content server
    Server server = CrawlTestUtil.getServer(conf
      .getInt("content.server.port",50000), "src/test/resources/fetch-test-site");
    server.start();

    // generate seed list
    ArrayList<String> urls = new ArrayList<>();
    addUrl(urls,"index.html",server);
    addUrl(urls,"pagea.html",server);
    addUrl(urls,"pageb.html",server);
    addUrl(urls,"dup_of_pagea.html",server);
    addUrl(urls,"nested_spider_trap.html",server);
    addUrl(urls,"exception.html",server);

    // inject
    site(0).inject(urls).get();

    // generate
    Map<NutchSite,Future<String>> batchIds =  new HashMap<>();
    for(NutchSite site : sites)
      batchIds.put(
        site,
        site.generate(0, System.currentTimeMillis(), false, false));

    // fetch
    long time = System.currentTimeMillis();
    List<Future<Integer>> futures = new ArrayList<>();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.fetch(batchId, 4, false, 4));
    }

    // check results
    for(Future<Integer> future : futures)
      future.get();

    // verify politeness, time taken should be more than (num_of_pages +1)*delay
    int minimumTime = (int) ((urls.size() + 1) * 1000 *
      conf.getFloat("fetcher.server.delay", 5));
    assertTrue((System.currentTimeMillis()-time) > minimumTime);

    // verify that enough pages were handled
    List<KeyWebPage> pages = readPageDB(Mark.FETCH_MARK);
    assertEquals(urls.size(), pages.size());
    List<String> handledurls = new ArrayList<>();
    for (KeyWebPage up : pages) {
      ByteBuffer bb = up.getDatum().getContent();
      if (bb == null) {
        continue;
      }
      String content = Bytes.toString(bb);
      if (content.contains("Nutch fetcher test page")) {
        handledurls.add(up.getDatum().getUrl());
      }
    }
    Collections.sort(urls);
    Collections.sort(handledurls);
    assertEquals(urls.size(), handledurls.size());
    assertTrue(handledurls.containsAll(urls));
    assertTrue(urls.containsAll(handledurls));

    server.stop();

  }
  
  @Test
  public void versionedCrawl() throws Exception {

    final int NPAGES = 1;
    final int DEGREE = 0;

    final int DEPTH = 3;
    final int WIDTH = 1;

    // set minimal refetch interval
    for (NutchSite site : sites) {
      site.getConf().set("db.fetch.interval.default", "0");
    }

    Configuration conf = NutchConfiguration.create();
    File tmpDir = Files.createTempDir();
    List<String> pages = createPages(NPAGES, DEGREE,
      tmpDir.getAbsolutePath());
    LOG.info("tmpDir abs path:"+tmpDir.getAbsolutePath());
    Server server = CrawlTestUtil.getServer(
      conf.getInt("content.server.port", 50000),
      tmpDir.getAbsolutePath());
    server.start();

    try {

      ArrayList<String> urls = new ArrayList<>();
      for (String page : pages) {
        if (urls.size()==WIDTH) break;
        addUrl(urls, page, server);
      }
      sites.get(0).inject(urls).get();

      List<Future<Integer>> futures = new ArrayList<>();
      for (NutchSite site : sites) {
        futures.add(site.crawl(WIDTH, DEPTH));
      }
      for(Future<Integer> future : futures)
        future.get();

      List<KeyWebPage> resultPages = readPageDB(null, "key", "markers");
      LOG.info("Pages: " + resultPages.size());
      if (DEPTH*WIDTH!=resultPages.size())
        LOG.warn("Depth*width: "+DEPTH*WIDTH);
      if (LOG.isDebugEnabled()) {
        for (KeyWebPage keyWebPage : resultPages) {
          System.out.println(keyWebPage.getDatum().getKey());
        }
      }
      assertEquals(DEPTH*WIDTH,resultPages.size());

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      long t0 = System.currentTimeMillis();
      FileUtils.deleteDirectory(tmpDir);
      long deleteTime = System.currentTimeMillis() - t0;
      LOG.info("Time to delete tmpDir: "+  deleteTime+"ms");
    }

    // set refetch interval to default value
    for (NutchSite site : sites) {
      site.getConf().set("db.fetch.interval.default", "2592000");
    }

  }

  @Test
  public void shortCrawl() throws Exception {

    Configuration conf = NutchConfiguration.create();

    // start content server
    Server server = CrawlTestUtil.getServer(conf
      .getInt("content.server.port",50000), "src/test/resources/fetch-test-site");
    server.start();

    // generate seed list
    ArrayList<String> urls = new ArrayList<>();
    addUrl(urls,"index.html",server);
    addUrl(urls,"pagea.html",server);
    addUrl(urls,"pageb.html",server);

    // inject
    site(0).inject(urls).get();

    assertEquals(3,readPageDB(null).size());

    // generate
    Map<NutchSite,Future<String>> batchIds =  new HashMap<>();
    for(NutchSite site : sites)
      batchIds.put(
        site,
        site.generate(0, System.currentTimeMillis(), false, false));

    // fetch
    List<Future<Integer>> futures = new ArrayList<>();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.fetch(batchId, 4, false, 0));
    }
    for(Future<Integer> future : futures)
      future.get();

    assertEquals(3,readPageDB(Mark.FETCH_MARK).size());

    // parse
    futures.clear();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.parse(batchId, false, false));
    }
    for(Future<Integer> future : futures)
      future.get();

    // update pageDB
    futures.clear();
    for(NutchSite site : sites) {
      String batchId = batchIds.get(site).get();
      futures.add(
        site.update(batchId));
    }
    for(Future<Integer> future : futures)
      future.get();

    assertEquals(3,readPageDB(Mark.UPDATEDB_MARK).size());

    // verify content
    List<KeyWebPage> pages = readPageDB(null);
    assertEquals(4, pages.size());
    for (KeyWebPage keyWebPage : pages) {
      if (keyWebPage.getKey().contains("index"))
        assertEquals(2, keyWebPage.getDatum().getInlinks().size());
      else if (keyWebPage.getKey().contains("page")) // pagea, pageb, dup_of_pagea
        assertEquals(1, keyWebPage.getDatum().getInlinks().size());
    }

    server.stop();

  }

  @Test
  public void longCrawl() throws  Exception{

    final int NPAGES = 1000;
    final int DEGREE = 10;

    final int DEPTH = 3;
    final int WIDTH = 100;

    Configuration conf = NutchConfiguration.create();
    File tmpDir = Files.createTempDir();
    List<String> pages = createPages(NPAGES, DEGREE,
      tmpDir.getAbsolutePath());
    LOG.info("tmpDir abs path:"+tmpDir.getAbsolutePath());
    Server server = CrawlTestUtil.getServer(
      conf.getInt("content.server.port", 50000),
      tmpDir.getAbsolutePath());
    server.start();
	
    try {
    
      ArrayList<String> urls = new ArrayList<>();
      for (String page : pages) {
        if (urls.size()==WIDTH) break;
        addUrl(urls, page, server);
      }
      sites.get(0).inject(urls).get();

      List<Future<Integer>> futures = new ArrayList<>();
      for (NutchSite site : sites) {
        futures.add(site.crawl(WIDTH, DEPTH));
      }
      for(Future<Integer> future : futures)
        future.get();

      List<KeyWebPage> resultPages = readPageDB(Mark.UPDATEDB_MARK, "key", "markers");
      LOG.info("Pages: " + resultPages.size());
      if (DEPTH*WIDTH!=resultPages.size())
        LOG.warn("Depth*width: "+DEPTH*WIDTH);
      if (LOG.isDebugEnabled()) {
        for (KeyWebPage keyWebPage : resultPages) {
          System.out.println(keyWebPage.getDatum().getKey());
        }
      }
    
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      long t0 = System.currentTimeMillis();
      FileUtils.deleteDirectory(tmpDir);
      long deleteTime = System.currentTimeMillis() - t0;
      LOG.info("Time to delete tmpDir: "+  deleteTime+"ms");
    }

  }

  @Test
  @Ignore
  public void realCrawl() throws  Exception {
    final int DEPTH = 3;
    final int WIDTH = 10;

    try {

      ArrayList<String> urls = new ArrayList<>();
      urls.add("http://www.fxstreet.com/technical-studies/currencies/eursek/");
      sites.get(0).inject(urls).get();

      List<Future<Integer>> futures = new ArrayList<>();
      for (NutchSite site : sites) {
        futures.add(site.crawl(WIDTH, DEPTH));
      }
      for(Future<Integer> future : futures)
        future.get();

      List<KeyWebPage> resultPages = readPageDB(Mark.UPDATEDB_MARK, "key", "markers", "score");
      LOG.info("Pages: " + resultPages.size());
      if (DEPTH*WIDTH!=resultPages.size())
        LOG.warn("Depth*width: "+DEPTH*WIDTH);
      // if (LOG.isDebugEnabled()) {
        for (KeyWebPage keyWebPage : resultPages) {
          System.out.println("<"+keyWebPage.getDatum().getKey()+","+keyWebPage.getDatum().getScore()+">");
        }
    // }

    } catch (Exception e) {
      e.printStackTrace();
    }
    
  }

  @Test
  public void verifyCrawlScore() throws Exception {

    final int DEPTH = 1;

    Configuration conf = NutchConfiguration.create();
    File tmpDir = Files.createTempDir();
    LOG.info("tmpDir abs path:"+tmpDir.getAbsolutePath());

    Map<String, String[]> graph = TestOPICScoringFilter.graph;

    for(String url : graph.keySet()) {
      ServerTestUtils.createPage(url, graph.get(url),tmpDir.getAbsolutePath());
    }

    Server server = CrawlTestUtil.getServer(
      conf.getInt("content.server.port", 50000),
      tmpDir.getAbsolutePath());
    server.start();

  try {

    List<String> seeds = new ArrayList<>();
    for (String url: graph.keySet()) {
      addUrl(seeds, url, server);
    }

    sites.get(0).inject(seeds).get();

    List<Future<Integer>> futures = new ArrayList<>();

    for (int round=1; round<=3; round++) {

      futures.clear();

      for (NutchSite site : sites) {
        futures.add(site.crawl(graph.keySet().size(), DEPTH));
      }

      for (Future<Integer> future : futures)
        future.get();

      List<KeyWebPage> resultPages = readLastVersionPageDB(
        null,
        "score", true,
        "key","url", "score");

      Float score = 0f;
      Map<String, WebPage> resultPagesMap = new TreeMap<>();
      for (KeyWebPage resultPage : resultPages) {
        WebPage page = resultPage.getDatum();
        assert score < page.getScore();
        score = page.getScore();
        System.out.println(
          "<"
            + page.getUrl() + ", "
            + page.getFetchTime() + ", "
            + page.getScore() + ">");
        resultPagesMap.put(page.getUrl(), page);
      }

      for (String url : resultPagesMap.keySet()) {
        String[] split = new URL(resultPagesMap.get(url).getUrl()).getPath().split("/");
        DecimalFormat df = new DecimalFormat("#.##");

        String result = df.format(resultPagesMap.get(url).getScore());
        String accepted = df.format(
          TestOPICScoringFilter.acceptedScores.get(round)
            .get(split[split.length - 1]));

        assert accepted.equals(result) : url + ": " + accepted + "vs" + result;

      }
    }

  } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.stop();
      long t0 = System.currentTimeMillis();
      FileUtils.deleteDirectory(tmpDir);
      long deleteTime = System.currentTimeMillis() - t0;
      LOG.info("Time to delete tmpDir: "+  deleteTime+"ms");
    }

  }


  // Helpers

  public int nbPages(){
    return 10;
  }

  public int nbLinks(){
    return 1;
  }

  public List<KeyWebPage> readPageDB(Mark requiredMark, String... fields)
    throws Exception {
    List<KeyWebPage> content = new ArrayList<>();
    for(NutchSite site : sites){
      content.addAll(site.readPageDB(requiredMark, fields));
    }
    return content;
  }

  public List<KeyWebPage> readLastVersionPageDB(Mark requiredMark,
    String sortingField, boolean isAscendant, String... fields)
    throws Exception  {
    List<KeyWebPage> result = new ArrayList<>();
      for(NutchSite site : sites) {
        result.addAll(site
          .readLastVersionPageDB(requiredMark, sortingField, isAscendant,
            fields));
      }
      return result;
  }

}
