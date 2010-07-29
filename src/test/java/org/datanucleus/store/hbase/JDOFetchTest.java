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
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.Flight;
import org.datanucleus.test.models.HasMultiValuePropsJDO;
import org.datanucleus.test.models.KitchenSink;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class JDOFetchTest extends BaseTest
{

    @BeforeClass
    public static void setup() throws IOException
    {
        final HBaseAdmin hBaseAdmin = new HBaseAdmin(new HBaseConfiguration());
        HTableDescriptor flightHTable = null;
        try
        {
            flightHTable = hBaseAdmin.getTableDescriptor(Bytes.toBytes("Flight"));
        }
        catch (TableNotFoundException ex)
        {
            flightHTable = new HTableDescriptor(Bytes.toBytes("Flight"));
            hBaseAdmin.createTable(flightHTable);
        }
        if (!flightHTable.hasFamily(Bytes.toBytes("Flight")))
        {
            HColumnDescriptor hColumn = new HColumnDescriptor(Bytes.toBytes("Flight"));
            flightHTable.addFamily(hColumn);
            hBaseAdmin.disableTable(flightHTable.getName());
            hBaseAdmin.modifyTable(flightHTable.getName(), flightHTable);
            hBaseAdmin.enableTable(flightHTable.getName());
        }

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

        HTableDescriptor muliValuPropsHTable = null;
        try
        {
            muliValuPropsHTable = hBaseAdmin.getTableDescriptor(Bytes.toBytes("HasMultiValuePropsJDO"));
        }
        catch (TableNotFoundException ex)
        {
            muliValuPropsHTable = new HTableDescriptor(Bytes.toBytes("HasMultiValuePropsJDO"));
            hBaseAdmin.createTable(muliValuPropsHTable);
        }
        if (!muliValuPropsHTable.hasFamily(Bytes.toBytes("HasMultiValuePropsJDO")))
        {
            HColumnDescriptor hColumn = new HColumnDescriptor(Bytes.toBytes("HasMultiValuePropsJDO"));
            muliValuPropsHTable.addFamily(hColumn);
            hBaseAdmin.disableTable(muliValuPropsHTable.getName());
            hBaseAdmin.modifyTable(muliValuPropsHTable.getName(), muliValuPropsHTable);
            hBaseAdmin.enableTable(muliValuPropsHTable.getName());
        }

    }

    @Test
    public void testSimpleFetch_Id() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        HTable table = new HTable(Bytes.toBytes("Flight"));
        table.put(Flight.newFlightPut("key0", "1", "yam", "bam", 1, 2, 300));
        beginTxn();
        Flight flight = pm.getObjectById(Flight.class, "key0");
        assertNotNull(flight);
        assertEquals("key0", flight.getId());
        assertEquals("yam", flight.getOrigin());
        assertEquals("bam", flight.getDest());
        assertEquals("1", flight.getName());
        assertEquals(1, flight.getYou());
        assertEquals(2, flight.getMe());
        commitTxn();
    }

    @Test
    public void testSimpleFetch_Id_LongIdOnly_NotFound()
    {
        PersistenceManager pm = getPersistenceManager();
        try
        {
            pm.getObjectById(Flight.class, "-1");
            fail("expected onfe");
        }
        catch (JDOObjectNotFoundException e)
        {
            // good
        }
    }

    @Test
    public void testFetchNonExistent() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        HTable table = new HTable(Bytes.toBytes("Flight"));
        table.put(Flight.newFlightPut("key0.5", "1", "yam", "bam", 1, 2, 300));
        table.delete(new Delete(Bytes.toBytes("key0.5")));
        table.close();
        try
        {
            pm.getObjectById(Flight.class, "key0.5");
            fail("expected onfe");
        }
        catch (JDOObjectNotFoundException onfe)
        {
            // good
        }
    }

    @Test
    public void testKitchenSinkFetch() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        HTable table = new HTable(Bytes.toBytes("KitchenSink"));
        table.put(KitchenSink.newKitchenSinkPut("key0.8"));
        beginTxn();
        KitchenSink ks = pm.getObjectById(KitchenSink.class, "key0.8");
        assertNotNull(ks);
        assertEquals("key0.8", ks.key);
        assertEquals(KitchenSink.newKitchenSink(ks.key), ks);
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key0.8"));
        table.delete(delete);
        table.close();
    }

    @Test
    public void testFetchSet() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key1"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strSet"), Utils.serializeObject(Utils.newHashSet("a", "b", "c")));
        HTable table = new HTable("HasMultiValuePropsJDO");
        table.put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key1");
        assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrSet());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key1"));
        table.delete(delete);
        table.close();
    }

    @Test
    public void testFetchArrayList() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key2"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strArrayList"),
            Utils.serializeObject(Utils.newArrayList("a", "b", "c")));
        HTable table = new HTable("HasMultiValuePropsJDO");
        table.put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key2");
        assertEquals(Utils.newArrayList("a", "b", "c"), pojo.getStrArrayList());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key2"));
        table.delete(delete);
        table.close();
    }

    @Test
    public void testFetchList() throws IOException
    {
        Put put = new Put(Bytes.toBytes("key3"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strList"), Utils.serializeObject(Utils.newArrayList("a", "b", "c")));
        HTable table = new HTable("HasMultiValuePropsJDO");
        table.put(put);

        PersistenceManager pm = getPersistenceManager();

        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key3");
        assertEquals(Utils.newArrayList("a", "b", "c"), pojo.getStrList());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key3"));
        table.delete(delete);
        table.close();
    }

    @Test
    public void testFetchLinkedList() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key4"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strLinkedList"),
            Utils.serializeObject(Utils.newLinkedList("a", "b", "c")));
        new HTable("HasMultiValuePropsJDO").put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key4");
        assertEquals(Utils.newLinkedList("a", "b", "c"), pojo.getStrLinkedList());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key4"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

    @Test
    public void testFetchHashSet() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key5"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strHashSet"), Utils.serializeObject(Utils.newHashSet("a", "b", "c")));
        new HTable("HasMultiValuePropsJDO").put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key5");
        assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrHashSet());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key5"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

    @Test
    public void testFetchLinkedHashSet() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key6"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strLinkedHashSet"),
            Utils.serializeObject(Utils.newLinkedHashSet("a", "b", "c")));
        new HTable("HasMultiValuePropsJDO").put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key6");
        assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrLinkedHashSet());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key6"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

    @Test
    public void testFetchSortedSet() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key7"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strSortedSet"),
            Utils.serializeObject(Utils.newTreeSet("a", "b", "c")));
        new HTable("HasMultiValuePropsJDO").put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key7");
        assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrSortedSet());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key7"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

    @Test
    public void testFetchTreeSet() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key8"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("strTreeSet"), Utils.serializeObject(Utils.newTreeSet("a", "b", "c")));
        new HTable("HasMultiValuePropsJDO").put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key8");
        assertEquals(Utils.newHashSet("a", "b", "c"), pojo.getStrTreeSet());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key8"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

    @Test
    public void testFetchCollection() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = new Put(Bytes.toBytes("key9"));
        put.add(Bytes.toBytes("HasMultiValuePropsJDO"), Bytes.toBytes("intColl"), Utils.serializeObject(Utils.newArrayList(2, 3, 4)));
        new HTable("HasMultiValuePropsJDO").put(put);
        beginTxn();
        HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, "key9");
        assertEquals(Utils.newArrayList(2, 3, 4), pojo.getIntColl());
        commitTxn();
        Delete delete = new Delete(Bytes.toBytes("key9"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

    @Test
    public void testNumberTooLarge() throws IOException
    {
        PersistenceManager pm = getPersistenceManager();
        Put put = Flight.newFlightPut("key10", "1", "yam", "bam", 1, 2, 300);
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("you"), Bytes.toBytes(Integer.MAX_VALUE + 1));
        new HTable(Bytes.toBytes("Flight")).put(put);
        Flight f = pm.getObjectById(Flight.class, "key10");
        // no exception, just overflow
        assertEquals(-2147483648, f.getYou());
        Delete delete = new Delete(Bytes.toBytes("key10"));
        new HTable(Bytes.toBytes("HasMultiValuePropsJDO")).delete(delete);
    }

}
