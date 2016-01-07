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
package org.apache.nutch.indexer.anchor;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * JUnit test case which tests
 * 1. that anchor text is obtained
 * 2. that anchor deduplication functionality is working
 *
 */
public class TestAnchorIndexingFilter {
  
  @Test
  public void testDeduplicateAnchor() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.setBoolean("anchorIndexingFilter.deduplicate", true);
    AnchorIndexingFilter filter = new AnchorIndexingFilter();
    filter.setConf(conf);
    NutchDocument doc = new NutchDocument();
    WebPage page = WebPage.newBuilder().build();
    page.getInlinks().put("http://example1.com/", "cool site");
    page.getInlinks().put("http://example2.com/", "cool site");
    page.getInlinks().put("http://example3.com/", "fun site");
    filter.filter(doc, "http://myurldoesnotmatter.com/", page);
    
    assertTrue("test if there is an anchor at all", doc.getFieldNames().contains("anchor"));
    
    assertEquals("test dedup, we expect 2", 2, doc.getFieldValues("anchor").size());
  }

}
