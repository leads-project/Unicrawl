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
package org.apache.nutch.storage;

public enum Mark {
  INJECT_MARK("_injmrk_"), GENERATE_MARK("_gnmrk_"), FETCH_MARK("_ftcmrk_"),
  PARSE_MARK("__prsmrk__"), UPDATEDB_MARK("_updmrk_"), INDEX_MARK("_idxmrk_");

  private String name;

  Mark(String name) {
    this.name = new String(name);
  }

  public void putMark(WebPage page, String markValue) {
      page.getMarkers().put(name, markValue);
  }
  
  public void putMark(WebPage page) {
    page.getMarkers().put(name, "");
  }

  public String removeMark(WebPage page) {
    return (String) page.getMarkers().put(name, null);
  }

  public String checkMark(WebPage page) {
    return (String) page.getMarkers().get(name);
  }
  
  public boolean containsMark(WebPage page){
    return page.getMarkers().containsKey(name);
    
  }

  /**
   * Remove the mark only if the mark is present on the page.
   * @param page The page to remove the mark from.
   * @return If the mark was present.
   */
  public String removeMarkIfExist(WebPage page) {
    if (checkMark(page) != null) {
      return removeMark(page);
    }
    return null;
  }
  
  public String getName() {
	return name;
  }
}
