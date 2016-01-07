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
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.WebPageWritable;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DbUpdateMapper
  extends GoraMapper<String, WebPage, UrlWithScore, NutchWritable> {
  public static final Logger LOG = DbUpdaterJob.LOG;

  private ScoringFilters scoringFilters;

  private final List<ScoreDatum> scoreData = new ArrayList<>();

  private String batchId;

  //reuse writables
  private UrlWithScore urlWithScore = new UrlWithScore();
  private NutchWritable nutchWritable = new NutchWritable();
  private WebPageWritable pageWritable;

  @Override
  public void map(String key, WebPage page, Context context)
    throws IOException, InterruptedException {

    String url = page.getUrl();

    if(Mark.GENERATE_MARK.checkMark(page) == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + url + "; not generated yet");
      }
      return;
    }

    scoreData.clear();
    Map<String, String> outlinks = page.getOutlinks();
    if (outlinks != null) {
      for (Entry<String, String> e : outlinks.entrySet()) {
        int depth=Integer.MAX_VALUE;
        String depthString = page.getMarkers().get(DbUpdaterJob.DISTANCE);
        if (depthString != null) depth=Integer.parseInt(depthString);
        scoreData.add(new ScoreDatum(0.0f, e.getKey(), e.getValue(), page.getFetchTime(), depth));
      }
    }

    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + url +
        " exception:" + StringUtils.stringifyException(e));
    }

    urlWithScore.setUrl(url);
    urlWithScore.setScore(Float.MAX_VALUE);
    pageWritable.setWebPage(page);
    nutchWritable.set(pageWritable);
    context.write(urlWithScore, nutchWritable);

    LOG.trace(page.toString());
    for (ScoreDatum scoreDatum : scoreData) {
      urlWithScore.setUrl(scoreDatum.getUrl());
      urlWithScore.setScore(scoreDatum.getScore());
      LOG.trace(scoreDatum.toString());
      scoreDatum.setUrl(url);
      nutchWritable.set(scoreDatum);
      context.write(urlWithScore, nutchWritable);
    }

  }

  @Override
  public void setup(Context context) {
    scoringFilters = new ScoringFilters(context.getConfiguration());
    pageWritable = new WebPageWritable(context.getConfiguration(), null);
    batchId = context.getConfiguration().get(Nutch.BATCH_NAME_KEY,Nutch.ALL_BATCH_ID_STR);
  }

}
