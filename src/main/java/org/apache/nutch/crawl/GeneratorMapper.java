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

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;

import java.io.IOException;
import java.net.MalformedURLException;

public class GeneratorMapper
extends GoraMapper<String, WebPage, SelectorEntry, WebPage> {

  private URLFilters filters;
  private URLNormalizers normalizers;
  private boolean filter;
  private boolean normalise;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private long curTime;
  private SelectorEntry entry;
  private int maxDistance;

  @Override
  public void map(String key, WebPage page,
      Context context) throws IOException, InterruptedException {
    
    String url = page.getUrl();
    long fetchTime = page.getFetchTime();

    //filter on distance
    if (maxDistance > -1) {
      String distanceString = page.getMarkers().get(DbUpdaterJob.DISTANCE);
      if (distanceString != null) {
        int distance=Integer.parseInt(distanceString);
        if (distance > maxDistance) {
          GeneratorJob.LOG.debug("Skipping " + url + "; too far");
          return;
        }
      }
    }

    // If filtering is on don't generate URLs that don't pass URLFilters
    try {
      if (normalise) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      }
      if (filter && filters.filter(url) == null)
        return;
    } catch (URLFilterException e) {
      if (GeneratorJob.LOG.isWarnEnabled()) {
        GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
        return;
      }
    } catch (MalformedURLException e) {
      if (GeneratorJob.LOG.isWarnEnabled()) {
        GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() +")");
        return;
      }
    }

    // check fetch schedule
    if (!schedule.shouldFetch(url, page, curTime)) {
      if (GeneratorJob.LOG.isDebugEnabled()) {
        GeneratorJob.LOG.debug("-shouldFetch rejected '" + url + "', fetchTime=" +
            page.getFetchTime() + ", curTime=" + curTime);
      }
      return;
    }
    
    float score = page.getScore();
    try {
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException e) {
      //ignore
    }
    entry = new SelectorEntry();
    entry.set(url, fetchTime, score);
    
    GeneratorJob.LOG.trace("Adding "+entry.toString());
    context.write(entry, page);
  }

  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
    normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
    if (filter) {
      filters = new URLFilters(conf);
    }
    if (normalise) {
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    }
    maxDistance=conf.getInt("generate.max.distance", -1);
    curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
  }
}
