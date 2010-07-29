/**********************************************************************
 * Copyright (c) 2010 Ghais Issa and others. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors :
 * ...
 ************************************************************************/
package org.datanucleus.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author ghais
 */
public class BaseTest
{

    private static final Log LOG = LogFactory.getLog(BaseTest.class);

    public static final String TEST_DIRECTORY_KEY = "test.build.data";

    static HBaseConfiguration conf = null;

    static MiniDFSCluster dfsCluster = null;

    static MiniHBaseCluster cluster = null;

    static MiniZooKeeperCluster zooKeeperCluster = null;

    private static final boolean OPEN_META_TABLE = true;

    static final int regionServers = 1;

    static HBaseAdmin admin;

    static Path testDir = null;

    static FileSystem fs = null;

    protected static ThreadLocal<PersistenceManager> pm = new ThreadLocal<PersistenceManager>();

    @Test
    public void doNothing()
    {
        // Don't produce an initialization error
    }

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        conf = new HBaseConfiguration();
        dfsCluster = new MiniDFSCluster(conf, 2, true, (String[]) null);

        // mangle the conf so that the fs parameter points to the minidfs we
        // just started up
        final FileSystem filesystem = dfsCluster.getFileSystem();
        conf.set("fs.default.name", filesystem.getUri().toString());
        final Path parentdir = filesystem.getHomeDirectory();
        conf.set(HConstants.HBASE_DIR, parentdir.toString());
        filesystem.mkdirs(parentdir);
        FSUtils.setVersion(filesystem, parentdir);

        preHBaseClusterSetup();
        hBaseClusterSetup();
        postHBaseClusterSetup();

        pm.set(PMF.get().getPersistenceManager());

    }

    /**
     * Run after dfs is ready but before hbase cluster is started up.
     */
    protected static void preHBaseClusterSetup() throws Exception
    {
    }

    /**
     * Actually start the MiniHBase instance.
     */
    protected static void hBaseClusterSetup() throws Exception
    {
        final File testDir = new File(getUnitTestdir(BaseTest.class.getName()).toString());

        // Note that this is done before we create the MiniHBaseCluster because
        // we
        // need to edit the config to add the ZooKeeper servers.
        zooKeeperCluster = new MiniZooKeeperCluster();
        final int clientPort = zooKeeperCluster.startup(testDir);
        conf.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));

        // start the mini cluster
        cluster = new MiniHBaseCluster(conf, regionServers);

        if (OPEN_META_TABLE)
        {
            // opening the META table ensures that cluster is running
            new HTable(conf, HConstants.META_TABLE_NAME);
        }
    }

    protected static Path getUnitTestdir(final String testName)
    {
        return new Path(conf.get(TEST_DIRECTORY_KEY, "build/test/data"), testName);
    }

    /**
     * Run after hbase cluster is started up.
     */
    protected static void postHBaseClusterSetup() throws Exception
    {

    }

    @AfterClass
    public static void afterClass() throws Exception
    {
        if (!pm.get().isClosed())
        {
            if (pm.get().currentTransaction().isActive())
            {
                pm.get().currentTransaction().rollback();
            }
            pm.get().close();
        }
        pm.remove();

        if (!OPEN_META_TABLE)
        {
            // open the META table now to ensure cluster is running before
            // shutdown.
            new HTable(conf, HConstants.META_TABLE_NAME);
        }
        try
        {
            HConnectionManager.deleteConnectionInfo(conf, true);
            if (cluster != null)
            {
                try
                {
                    cluster.shutdown();
                }
                catch (final Exception e)
                {
                    LOG.warn("Closing mini dfs", e);
                }
                try
                {
                    zooKeeperCluster.shutdown();
                }
                catch (final IOException e)
                {
                    LOG.warn("Shutting down ZooKeeper cluster", e);
                }
            }
            HBaseTestCase.shutdownDfs(dfsCluster);
        }
        catch (final Exception e)
        {
            LOG.error(e);
        }

    }

    public static final class PMF
    {
        private final static Properties properties;
        /*
         * Initialize the properties. The ConnectionUrl is retrieved from ZooKeeper therefore hbase-site.xml should be
         * on the classpath.
         */
        static
        {
            properties = new Properties();
            // TODO: Lazy loading doesn't work
            properties.setProperty("datanucleus.cache.collections.lazy", "false");
            properties.setProperty("javax.jdo.PersistenceManagerFactoryClass", "org.datanucleus.jdo.JDOPersistenceManagerFactory");
            // TODO:Add L2 Cache
            properties.setProperty("javax.jdo.option.ConnectionURL", "hbase:");
            properties.setProperty("javax.jdo.option.ConnectionUserName", "");
            properties.setProperty("javax.jdo.option.ConnectionPassword", "");
            properties.setProperty("datanucleus.autoCreateTables", "true");
            properties.setProperty("datanucleus.autoCreateSchema", "true");
            properties.setProperty("hbase.ignorableMetaDataBehavior", "WARN");
        }

        private static final PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(properties);

        /**
         * @return The {@link PersistenceManagerFactory}
         */

        public static PersistenceManagerFactory get()
        {
            return pmf;
        }

        /**
         * Default privat constructor
         */
        private PMF()
        {

        }
    }

    protected Result get(String tableName, byte[] key) throws IOException
    {
        Get get = new Get(key);
        HTable table = new HTable(Bytes.toBytes(tableName));
        return table.get(get);
    }

    @SuppressWarnings("unchecked")
    protected <T> T deserialize(Class<T> cls, byte[] bytes) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (T) ois.readObject();
    }

    protected void beginTxn()
    {
        pm.get().currentTransaction().begin();
    }

    protected void commitTxn()
    {
        pm.get().currentTransaction().commit();
    }

    protected void rollbackTxn()
    {
        pm.get().currentTransaction().rollback();
    }

    protected String getStrValue(Result entity, String family, String qualifier)
    {
        return Bytes.toString(entity.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
    }

    protected int getIntValue(Result entity, String family, String qualifier)
    {
        return Bytes.toInt(entity.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
    }

    protected long getLongValue(Result entity, String family, String qualifier)
    {
        return Bytes.toLong(entity.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
    }

    protected byte[] getBytes(Result entity, String family, String qualifier)
    {
        return entity.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    /**
     * Use this only for testing, it may take a long time
     * @param tableName
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unused")
    protected int countForTable(String tableName) throws IOException
    {
        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes(tableName));
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result result : scanner)
        {
            count++;
        }
        return count;
    }

    protected PersistenceManager getPersistenceManager()
    {
        return pm.get();
    }

    interface StartEnd
    {
        void start();

        void end();
    }

    public final StartEnd TXN_START_END = new StartEnd()
    {
        public void start()
        {
            beginTxn();
        }

        public void end()
        {
            commitTxn();
        }
    };

    protected <T> T makePersistentInTxn(T obj, StartEnd startEnd)
    {
        boolean success = false;
        startEnd.start();
        try
        {
            pm.get().makePersistent(obj);
            startEnd.end();
            success = true;
        }
        finally
        {
            if (!success && pm.get().currentTransaction().isActive())
            {
                rollbackTxn();
            }
        }
        return obj;
    }
}
