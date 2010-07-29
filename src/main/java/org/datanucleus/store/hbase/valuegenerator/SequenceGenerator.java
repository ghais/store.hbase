/**********************************************************************
Copyright (c) 2010 Ghais Issa and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package org.datanucleus.store.hbase.valuegenerator;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.hbase.FatalNucleusUserException;
import org.datanucleus.store.hbase.HBaseConnectionPool;
import org.datanucleus.store.hbase.HBaseManagedConnection;
import org.datanucleus.store.hbase.Utils;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;

/**
 * @author Ghais Issa
 */
public class SequenceGenerator extends AbstractDatastoreGenerator
{

    private static HBaseConnectionPool pool = new HBaseConnectionPool();

    private final static byte[] defaultName = Bytes.toBytes("__sequence__");

    private String tableName;

    private byte[] columnFamily;

    private byte[] qualifier;

    private byte[] sequenceName;

    // TODO(maxr): Get rid of this when the local datastore id allocation behavior
    // mirrors prod
    private static final ThreadLocal<String> SEQUENCE_POSTFIX_APPENDAGE = new ThreadLocal<String>()
    {
        @Override
        protected String initialValue()
        {
            return "";
        }
    };

    /**
     * @param name
     * @param props
     */
    public SequenceGenerator(String name, Properties props)
    {
        super(name, props);
        OMFContext omfContext = getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        ClassLoaderResolver clr = omfContext.getClassLoaderResolver(getClass().getClassLoader());
        AbstractClassMetaData acmd = mdm.getMetaDataForClass((String) properties.get("class-name"), clr);

        tableName = Utils.getTableName(acmd);
        columnFamily = Bytes.toBytes(properties.getProperty("column-name"));
        if (columnFamily == null)
        {
            columnFamily = Bytes.toBytes(tableName);
        }
        qualifier = Bytes.toBytes(properties.getProperty("field-name"));
        if (qualifier == null)
        {
            qualifier = defaultName;
        }

        sequenceName = Bytes.toBytes(properties.getProperty("sequence-name"));
        if (sequenceName == null)
        {
            sequenceName = defaultName;
        }

    }

    /**
     * @Override public void setStoreManager(StoreManager storeMgr) { super.setStoreManager(storeMgr); OMFContext
     * omfContext = getOMFContext(); MetaDataManager mdm = omfContext.getMetaDataManager(); ClassLoaderResolver clr =
     * omfContext.getClassLoaderResolver(getClass().getClassLoader()); AbstractClassMetaData acmd =
     * mdm.getMetaDataForClass((String) properties.get("class-name"), clr); if (acmd != null) { ((HBaseStoreManager)
     * storeMgr).validateMetaDataForClass(acmd, clr); } sequenceName = determineSequenceName(acmd); if (sequenceName !=
     * null) { // Fetch the sequence data SequenceMetaData sequenceMetaData = mdm.getMetaDataForSequence(clr,
     * sequenceName); if (sequenceMetaData != null) { // derive allocation size and sequence name from the sequence meta
     * data if (sequenceMetaData.hasExtension(KEY_CACHE_SIZE_PROPERTY)) { allocationSize =
     * Integer.parseInt(sequenceMetaData.getValueForExtension(KEY_CACHE_SIZE_PROPERTY)); } else { allocationSize =
     * longToInt(sequenceMetaData.getAllocationSize()); } sequenceName = sequenceMetaData.getDatastoreSequence(); } else
     * { // key cache size is passed in as a prop for JDO when the sequence // is used directly (pm.getSequence()) if
     * (properties.getProperty(KEY_CACHE_SIZE_PROPERTY) != null) { allocationSize =
     * Integer.parseInt(properties.getProperty(KEY_CACHE_SIZE_PROPERTY)); } } } // derive the sequence name from the
     * class meta data if (sequenceName == null) { sequenceName = deriveSequenceNameFromClassMetaData(acmd); } }
     */

    private OMFContext getOMFContext()
    {
        return storeMgr.getOMFContext();
    }

    protected ValueGenerationBlock reserveBlock(long size)
    {
        if (tableName == null || columnFamily == null || qualifier == null)
        {
            // shouldn't happen
            throw new IllegalStateException("sequence name is null");
        }
        long currentMax = 0;
        HBaseManagedConnection connection = pool.getPooledConnection();
        HTable table = connection.getHTable(tableName);
        try
        {
            currentMax = table.incrementColumnValue(sequenceName, columnFamily, qualifier, size, true);
        }
        catch (IOException e)
        {
            throw new FatalNucleusUserException("couldn't reserve block", e);
        }
        finally
        {
            connection.release();
        }

        // Too bad we can't pass an iterable and construct the ids
        // on demand.
        List<Long> ids = Utils.newArrayList();

        for (int i = 0; i < size; i++)
        {
            ids.add(currentMax - size + i);
        }
        return new ValueGenerationBlock(ids);
    }

    public static void setSequencePostfixAppendage(String appendage)
    {
        SEQUENCE_POSTFIX_APPENDAGE.set(appendage);
    }

    public static void clearSequencePostfixAppendage()
    {
        SEQUENCE_POSTFIX_APPENDAGE.remove();
    }

}
