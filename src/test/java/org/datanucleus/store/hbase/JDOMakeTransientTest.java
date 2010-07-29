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
package org.datanucleus.store.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.jdo.PersistenceManager;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.HasMultiValuePropsJDO;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class JDOMakeTransientTest extends BaseTest
{

    @Before
    public void setup() throws IOException
    {
        final HBaseAdmin hBaseAdmin = new HBaseAdmin(new HBaseConfiguration());
        HTableDescriptor table = null;
        try
        {
            table = hBaseAdmin.getTableDescriptor(Bytes.toBytes("HasMultiValuePropsJDO"));
        }
        catch (TableNotFoundException ex)
        {
            table = new HTableDescriptor(Bytes.toBytes("HasMultiValuePropsJDO"));
            hBaseAdmin.createTable(table);
        }
        if (!table.hasFamily(Bytes.toBytes("HasMultiValuePropsJDO")))
        {
            HColumnDescriptor hColumn = new HColumnDescriptor(Bytes.toBytes("HasMultiValuePropsJDO"));
            table.addFamily(hColumn);
            hBaseAdmin.disableTable(table.getName());
            hBaseAdmin.modifyTable(table.getName(), table);
            hBaseAdmin.enableTable(table.getName());
        }
    }

    @Test
    public void testListAccessibleOutsideTxn() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("id"), Bytes.toBytes("key"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strList"), Utils.serializeObject(Utils.newArrayList("a", "b", "c")));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("str"), Bytes.toBytes("yar"));

        new HTable("HasMultiValuePropsJDO").put(put);

        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key");

        pojo.setStr("yip");
        pojo.getStrList();
        commitTxn();
        assertEquals("yip", pojo.getStr());
        assertEquals(3, pojo.getStrList().size());

    }
}
