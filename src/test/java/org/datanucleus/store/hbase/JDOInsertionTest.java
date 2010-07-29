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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.jdo.PersistenceManager;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.Flight;
import org.datanucleus.test.models.HasKeyPkJDO;
import org.datanucleus.test.models.KitchenSink;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class JDOInsertionTest extends BaseTest
{

    @Before
    public void setup() throws IOException
    {
        final HBaseAdmin hBaseAdmin = new HBaseAdmin(new HBaseConfiguration());
        HTableDescriptor kitchenHTable = null;
        try
        {
            kitchenHTable = hBaseAdmin.getTableDescriptor(Bytes.toBytes("KitchenSink"));
        }
        catch (TableNotFoundException ex)
        {
            kitchenHTable = new HTableDescriptor(Bytes.toBytes("KitchenSink"));
            hBaseAdmin.createTable(kitchenHTable);
        }
        if (!kitchenHTable.hasFamily(Bytes.toBytes("KitchenSink")))
        {
            HColumnDescriptor hColumn = new HColumnDescriptor(Bytes.toBytes("KitchenSink"));
            kitchenHTable.addFamily(hColumn);
            hBaseAdmin.disableTable(kitchenHTable.getName());
            hBaseAdmin.modifyTable(kitchenHTable.getName(), kitchenHTable);
            hBaseAdmin.enableTable(kitchenHTable.getName());
        }
    }

    @Test
    public void testSimpleInsert() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Flight f1 = new Flight();
        f1.setOrigin("BOS");
        f1.setDest("MIA");
        f1.setMe(2);
        f1.setYou(4);
        f1.setName("Harold");
        assertNull(f1.getId());
        pm.makePersistent(f1);
        assertNotNull(f1.getId());

        Result entity = get("Flight", Bytes.toBytes(f1.getId()));
        assertNotSame(0, entity.size());
        assertEquals("BOS", getStrValue(entity, "Flight", "origin"));
        assertEquals("MIA", getStrValue(entity, "Flight", "dest"));
        assertEquals("Harold", getStrValue(entity, "Flight", "name"));
        assertEquals(2, getIntValue(entity, "Flight", "me"));
        assertEquals(4, getIntValue(entity, "Flight", "you"));
        Delete delete = new Delete(Bytes.toBytes(f1.getId()));
        new HTable("Flight").delete(delete);
    }

    @Test
    public void testSimpleInsertWithNamedKey() throws IOException
    {
        KitchenSink ks = KitchenSink.newKitchenSink("key");
        makePersistentInTxn(ks, TXN_START_END);
        Result entity = get("KitchenSink", Bytes.toBytes("key"));
        assertNotSame(0, entity.size());
        Delete delete = new Delete(Bytes.toBytes("key"));
        new HTable("KitchenSink").delete(delete);

    }

    @Test
    public void testKitchenSinkInsert() throws IOException
    {
        KitchenSink ks = KitchenSink.newKitchenSink("key");
        makePersistentInTxn(ks, TXN_START_END);
        Result entity = get("KitchenSink", Bytes.toBytes("key"));
        assertNotSame(0, entity.size());
        assertNull(getStrValue(entity, "KitchenSink", "an extra property"));
        Put put = KitchenSink.newKitchenSinkPut("key");
        for (KeyValue kv : entity.list())
        {
            assertTrue(put.has(kv.getFamily(), kv.getQualifier(), kv.getValue()));
        }
        Delete delete = new Delete(Bytes.toBytes("key"));
        new HTable("KitchenSink").delete(delete);
    }

    @Test
    public void testKitchenSinkInsertWithNulls() throws IOException
    {
        KitchenSink allNulls = new KitchenSink();
        allNulls.key = "key";
        makePersistentInTxn(allNulls, TXN_START_END);
        Result entity = get("KitchenSink", Bytes.toBytes(allNulls.key));
        assertEquals(9, entity.size()); // 9 is the number of primative fields which are stored as their default values
        KitchenSink noNulls = KitchenSink.newKitchenSink();
        makePersistentInTxn(noNulls, TXN_START_END);
        Result entityWithoutNulls = get("KitchenSink", Bytes.toBytes(noNulls.key));
        Put put = KitchenSink.newKitchenSinkPut(noNulls.key);
        for (KeyValue kv : entityWithoutNulls.list())
        {
            assertTrue(put.has(kv.getFamily(), kv.getQualifier(), kv.getValue()));
        }
        Delete delete = new Delete(Bytes.toBytes("key"));
        new HTable("KitchenSink").delete(delete);
        delete = new Delete(Bytes.toBytes(noNulls.key));
        new HTable("KitchenSink").delete(delete);

    }

    @Test
    public void testInsertWithKeyPk()
    {
        PersistenceManager pm = getPersistenceManager();
        HasKeyPkJDO hk = new HasKeyPkJDO();

        beginTxn();
        pm.makePersistent(hk);

        assertNotNull(hk.getKey());
        assertNull(hk.getStr());
        commitTxn();
    }

    @Test
    public void testMultipleInserts()
    {
        PersistenceManager pm = getPersistenceManager();
        Flight f1 = new Flight();
        f1.setOrigin("BOS");
        f1.setDest("MIA");
        f1.setMe(2);
        f1.setYou(4);
        f1.setName("Harold");

        Flight f2 = new Flight();
        f2.setOrigin("BOS");
        f2.setDest("MIA");
        f2.setMe(2);
        f2.setYou(4);
        f2.setName("Harold");

        beginTxn();
        pm.makePersistent(f1);
        pm.makePersistent(f2);
        commitTxn();

    }
}
