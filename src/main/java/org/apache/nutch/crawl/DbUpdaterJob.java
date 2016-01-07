/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.crawl;

import org.apache.gora.filter.Filter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.UrlWithScore.UrlOnlyPartitioner;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator.UrlOnlyComparator;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.ALL_CRAWL_ID;
import static org.apache.nutch.util.FilterUtils.getBatchIdFilter;
import static org.apache.nutch.util.FilterUtils.getFetchedFilter;

public class DbUpdaterJob extends NutchTool {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdaterJob.class);
  public static final String DISTANCE = "dist";

  public static enum probes{
    UPDATED_PAGES,
    UPDATED_LINKS,
    NEW_PAGES
  }

  public DbUpdaterJob() {

  }

  public DbUpdaterJob(Configuration conf) {
    setConf(conf);
  }
    
  public Map<String,Object> run(Map<String,Object> args) throws Exception {
    String crawlId = (String)args.get(Nutch.ARG_CRAWL);
    String batchId = (String)args.get(Nutch.ARG_BATCH);
    numJobs = 1;
    currentJobNum = 0;
    
    if (batchId == null) {
      batchId = Nutch.ALL_BATCH_ID_STR;
    }
    getConf().set(Nutch.BATCH_NAME_KEY, batchId);
    //job.setBoolean(ALL, updateAll);
    ScoringFilters scoringFilters = new ScoringFilters(getConf());

    currentJob = new NutchJob(getConf(), "update-table");
    if (crawlId != null) {
      currentJob.getConfiguration().set(Nutch.CRAWL_ID_KEY, crawlId);
    }
    
    // Partition by {url}, sort by {url,score} and group by {url}.
    // This ensures that the inlinks are sorted by score when they enter
    // the reducer.
    
    currentJob.setPartitionerClass(UrlOnlyPartitioner.class);
    currentJob.setSortComparatorClass(UrlScoreComparator.class);
    currentJob.setGroupingComparatorClass(UrlOnlyComparator.class);

    Filter<String, WebPage> filter;
    if ( batchId.equals(ALL_CRAWL_ID))
      filter = getFetchedFilter();
    else
      filter = getBatchIdFilter(batchId, Mark.FETCH_MARK);

    StorageUtils.initMapperJob(
      currentJob,
      null,
      UrlWithScore.class,
      NutchWritable.class,
      DbUpdateMapper.class,
      filter);

    currentJob.setReducerClass(DbUpdateReducer.class);

    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  private int updateTable(String crawlId,String batchId) throws Exception {
    
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.debug("DbUpdaterJob: starting at " + sdf.format(start));
    
    LOG.info("DbUpdaterJob: batchId: " + batchId);

    run(ToolUtil.toArgMap(Nutch.ARG_CRAWL, crawlId,
            Nutch.ARG_BATCH, batchId));
    
    long finish = System.currentTimeMillis();
    LOG.info("DbUpdaterJob: updated page(s): "+ currentJob.getCounters().findCounter(probes.UPDATED_PAGES).getValue());
    LOG.info("DbUpdaterJob: updated link(s): "+ currentJob.getCounters().findCounter(probes.UPDATED_LINKS).getValue());
    LOG.info("DbUpdaterJob: new page(s): "+ currentJob.getCounters().findCounter(probes.NEW_PAGES).getValue());

    LOG.info("DbUpdaterJob: finished at " + sdf.format(finish) + ", time elapsed: " + TimingUtil.elapsedTime(start, finish));
    return 0;
  }

  public int run(String[] args) throws Exception {
    String crawlId = null;
    String batchId;

    String usage = "Usage: DbUpdaterJob (<batchId> | -all) [-crawlId <id>] [-topN <n>" +
      "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n" +
      "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n";
    
    if (args.length == 0) {
      System.err.println(usage);
      return -1;
    }

    batchId = args[0];
    if (!batchId.equals("-all") && batchId.startsWith("-")) {
      System.err.println(usage);
      return -1;
    }

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      } if ("-topN".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      }else {
        throw new IllegalArgumentException("arg " +args[i]+ " not recognized");
      }
    }
    return updateTable(crawlId,batchId);
  }

  /**
   *
   * @param batchId
   * @return 0 if successful
   */
  public int update(String batchId) throws Exception {
    return updateTable(getConf().get(Nutch.CRAWL_ID_KEY),batchId);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new DbUpdaterJob(), args);
    System.exit(res);
  }

}
