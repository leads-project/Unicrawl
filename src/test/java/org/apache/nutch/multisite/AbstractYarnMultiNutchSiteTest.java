package org.apache.nutch.multisite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import java.io.File;

public abstract class AbstractYarnMultiNutchSiteTest extends AbstractMultiNutchSiteTest{

  protected MiniDFSCluster hdfsCluster;
  protected MiniYARNCluster yarnCluster;

    public void setUpClass() throws Exception {
      super.setUpClass();

      // HDFS creation
      File baseDir = new File("/tmp/hdfs").getAbsoluteFile();
      FileUtil.fullyDelete(baseDir);
      Configuration hdfsConf = new Configuration();
      hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdfsConf);
      hdfsCluster = builder.build();

      // YARN creation
      YarnConfiguration yarnConf = new YarnConfiguration();
      yarnConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
      yarnConf.setClass(
        YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class,
        ResourceScheduler.class);
      yarnCluster = new MiniYARNCluster("cluster", 1, 1, 1);
      yarnCluster.init(hdfsConf);
      yarnCluster.start();
    }

    public void tearDownClass() throws Exception {
      super.tearDownClass();
      hdfsCluster.shutdown();
      yarnCluster.getNodeManager(0).stop();
      yarnCluster.getResourceManager().stop();
    }

}
