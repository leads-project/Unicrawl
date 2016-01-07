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

import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.*;
import org.apache.nutch.fetcher.FetcherJob.FetcherMapper;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.nutch.crawl.GeneratorJob.*;

/** Reduce class for generate
 *
 * The #reduce() method write a random integer to all generated URLs. This random
 * number is then used by {@link FetcherMapper}.
 *
 */
public class GeneratorReducer
extends GoraReducer<SelectorEntry, WebPage, String, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(GeneratorJob.class);

  protected long count = 0;
  private long limit;
  private long maxCount;
  private boolean byDomain = false;
  private Map<String, Integer> hostCountMap = new HashMap<String, Integer>();
  private Set<String> generated = new HashSet<>();
  private String batchId;
  private FetchSchedule schedule;

  @Override
  protected void reduce(SelectorEntry entry, Iterable<WebPage> values,
      Context context) throws IOException, InterruptedException {

    for (WebPage page : values) {

      if (limit != 0 && count >= limit) {
        GeneratorJob.LOG
          .trace("Skipping " + page.getKey() + "; too many generated urls");
        context.getCounter("Generator", "LIMIT").increment(1);
        return;
      }

      if (generated.contains(entry.getUrl())) {
        GeneratorJob.LOG
          .trace("Skipping " + page.getUrl() + "; already generated");
        context.getCounter("Generator", "DUPLICATE").increment(1);
        continue;
      }

      if (maxCount > 0) {
        String hostordomain;
        if (byDomain) {
          hostordomain = URLUtil.getDomainName(entry.getUrl());
        } else {
          hostordomain = URLUtil.getHost(entry.getUrl());
        }

        Integer hostCount = hostCountMap.get(hostordomain);
        if (hostCount == null) {
          hostCountMap.put(hostordomain, 0);
          hostCount = 0;
        }
        if (hostCount >= maxCount) {
          context.getCounter("Generator", "HOST_LIMIT").increment(1);
          return;
        }
        hostCountMap.put(hostordomain, hostCount + 1);
      }

      // if already fetched, create a new version by changing the key
      if (Mark.FETCH_MARK.containsMark(page)) {
        page.setKey(TableUtil.computeKey(page));
      }

      Mark.GENERATE_MARK.putMark(page, batchId);
      page.setBatchId(batchId);
      try {
        context.write(page.getKey(), page);
        generated.add(page.getUrl());
      } catch (MalformedURLException e) {
        context.getCounter("Generator", "MALFORMED_URL").increment(1);
        continue;
      }
      
    }

    context.getCounter("Generator", "GENERATE_MARK").increment(1);
    LOG.trace("GeneratedUrl : " + entry.getUrl());
    count++;

  }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    limit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, 0);
    GeneratorJob.LOG.info("Limit (per reducer):"+limit);
    maxCount = conf.getLong(GENERATOR_MAX_COUNT, 0);
    GeneratorJob.LOG.info("Maxcount (per host):"+maxCount);
    batchId = conf.get(BATCH_ID);
    String countMode = conf.get(GENERATOR_COUNT_MODE, GENERATOR_COUNT_VALUE_HOST);
    if (countMode.equals(GENERATOR_COUNT_VALUE_DOMAIN)) {
      byDomain = true;
    }
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
  }

}
