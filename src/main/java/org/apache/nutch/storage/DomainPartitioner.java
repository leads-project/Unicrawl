package org.apache.nutch.storage;

import org.apache.nutch.util.URLUtil;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.ensemble.cache.distributed.partitioning.ClusteringBasedPartitioner;
import org.infinispan.ensemble.cache.distributed.partitioning.Coordinates;

import java.net.InetAddress;
import java.net.URL;
import java.util.List;


/**
  *
 * FIXME go for a square reducing technique or a quad tree approach
 *
 * @author Pierre Sutra
 * @since 4.0
 */
public class DomainPartitioner extends ClusteringBasedPartitioner<String,WebPage> {

  /**
   * @param ensembleCaches  the caches to partition
   * @param location        cache to store the locations of the keys
   */
  public DomainPartitioner(List<EnsembleCache<String, WebPage>> ensembleCaches, EnsembleCache<String, Coordinates> location) {
    super(ensembleCaches, location);
  }

  @Override
  protected Coordinates buildCoordinates(String s) {
    Coordinates ret = Coordinates.newBuilder().build();
    URL url = null;
    try {
      url = new URL(s);
      String domain = URLUtil.getDomainName(url);
      String ip = InetAddress.getByName(domain).getHostAddress();
      IPLocator locator = IPLocator.locate(ip);
      ret.setLatitude((double) locator.getLatitude());
      ret.setLongitude((double) locator.getLongitude());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret;
  }

}
