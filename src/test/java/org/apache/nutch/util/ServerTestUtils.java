package org.apache.nutch.util;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Pierre Sutra
 *
 */
public class ServerTestUtils {

  public static List<String> createPages(int nPages, int degree, String dir)
    throws Exception {

    String header ="<html><head><title>title</title></head><body>";
    String footer ="</body></html>";

    List<String> urls = new ArrayList<>();
    for (int i = 0; i < nPages; i++) {
      urls.add(i+".html");
    }

    for (String url : urls) {
      Path path = FileSystems.getDefault().getPath(dir+ "/" + url);
      String page = header;
      List<String> copy = new ArrayList<>(urls);
      copy.remove(url);
      Collections.shuffle(copy);
      for (String link : copy.subList(0, degree)) {
        page+="<a href=\""+link+"\">link</a>";
      }
      page+=footer;
      Files.write(path, page.getBytes());
    }

    return urls;

  }

  public static void createPage(String url, String []links, String dir)
    throws IOException {
    Path path = FileSystems.getDefault().getPath(dir+ "/" + url);

    String header ="<html><head><title>title</title></head><body>";
    String footer ="</body></html>";
    String page = header;

    for (String link : links) {
      page+="<a href=\""+link+"\">link</a>";
    }
    page+=footer;
    Files.write(path, page.getBytes());

  }

}
