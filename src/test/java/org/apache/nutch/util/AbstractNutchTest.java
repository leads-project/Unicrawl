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
package org.apache.nutch.util;

import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * This class provides common routines for setup/teardown of an in-memory data
 * store.
 */
public class AbstractNutchTest {

  public static final Logger LOG = LoggerFactory.getLogger(AbstractNutchTest.class);

  protected Configuration conf;
  protected FileSystem fs;
  protected Path testdir = new Path("build/test/working");
  protected DataStore<String, WebPage> webPageStore;
  protected boolean persistentDataStore = false;

  public AbstractNutchTest() throws IOException {
    super();
  }

  @Before
  public void setUp() throws Exception {
    LOG.info("Setting up Hadoop Test Case...");
    try {
      conf = NutchConfiguration.create();
      fs = FileSystem.get(conf);
      webPageStore = StorageUtils.createStore(conf, String.class, WebPage.class);
      if (!persistentDataStore)
        webPageStore.deleteSchema();
    } catch (Exception e) {
      LOG.error("Hadoop Test Case set up failed", e);
      // cleanup
      tearDown();
      throw new RuntimeException();
    }

  }

  @After
  public void tearDown() throws Exception {
    fs.delete(testdir, true);
  }

}
