/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.fetcher;

import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.KeyWebPage;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.CrawlTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Basic fetcher test
 * 1. generate seedlist
 * 2. inject
 * 3. generate
 * 3. fetch
 * 4. Verify contents
 *
 */
public class TestFetcher extends AbstractNutchTest {

  final static Path testdir=new Path("build/test/fetch-test");
  Path urlPath;
  Server server;

  public TestFetcher() throws IOException {}

  @Override
  @Before
  public void setUp() throws Exception{
    super.setUp();
    urlPath = new Path(testdir, "urls");
    server = CrawlTestUtil.getServer(conf.getInt("content.server.port",50000), "src/test/resources/fetch-test-site");
    server.start();
  }

  @Override
  @After
  public void tearDown() throws Exception{
    server.stop();
    fs.delete(testdir, true);
  }

  @Test
  public void testFetch() throws Exception {

    //generate seedlist
    ArrayList<String> urls = new ArrayList<String>();

    addUrl(urls,"index.html",server);
    addUrl(urls,"pagea.html",server);
    addUrl(urls,"pageb.html",server);
    addUrl(urls,"dup_of_pagea.html",server);
    addUrl(urls,"nested_spider_trap.html",server);
    addUrl(urls,"exception.html",server);

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    //inject
    InjectorJob injector = new InjectorJob(conf);
    injector.inject(urlPath);

    //generate
    long time = System.currentTimeMillis();
    GeneratorJob g = new GeneratorJob(conf);
    String batchId = g.generate(Long.MAX_VALUE, time, false, false);

    ArrayList<KeyWebPage> l = CrawlTestUtil.readPageDB(webPageStore,
      Mark.GENERATE_MARK);

    assertTrue(
      CrawlTestUtil.readPageDB(webPageStore, Mark.GENERATE_MARK).size() == 6);

    //fetch
    time = System.currentTimeMillis();
    conf.setBoolean(FetcherJob.PARSE_KEY, true);
    FetcherJob fetcher = new FetcherJob(conf);
    fetcher.fetch(batchId, 1, false, -1);

    time = System.currentTimeMillis() - time;

    //verify politeness, time taken should be more than (num_of_pages +1)*delay
    int minimumTime = (int) ((urls.size() + 1) * 1000 *
        conf.getFloat("fetcher.server.delay", 5));
    assertTrue(time > minimumTime);

    List<KeyWebPage> pages = CrawlTestUtil.readPageDB(webPageStore,
      Mark.FETCH_MARK);
    assertEquals(urls.size(), pages.size());
    List<String> handledurls = new ArrayList<String>();
    for (KeyWebPage up : pages) {
      ByteBuffer bb = up.getDatum().getContent();
      if (bb == null) {
        continue;
      }
      String content = Bytes.toString(bb);
      if (content.indexOf("Nutch fetcher test page")!=-1) {
        handledurls.add(up.getKey());
      }
    }
    Collections.sort(urls);
    Collections.sort(handledurls);

    //verify that enough pages were handled
    assertEquals(urls.size(), handledurls.size());

    //verify that correct pages were handled
    assertTrue(handledurls.containsAll(urls));
    assertTrue(urls.containsAll(handledurls));
  }

  public static void addUrl(Collection<String> collection, String url, Server server) {
    collection.add(
      "http://127.0.0.1:" + server.getConnectors()[0].getPort() + "/" + url);
  }

  @Test
  public void testAgentNameCheck() {

    boolean failedNoAgentName = false;
    conf.set("http.agent.name", "");

    try {
      conf.setBoolean(FetcherJob.PARSE_KEY, true);
      FetcherJob fetcher = new FetcherJob(conf);
      fetcher.checkConfiguration();
    } catch (IllegalArgumentException iae) {
      String message = iae.getMessage();
      failedNoAgentName = message.equals("Fetcher: No agents listed in "
          + "'http.agent.name' property.");
    } catch (Exception e) {
    }

    assertTrue(failedNoAgentName);
  }

}
