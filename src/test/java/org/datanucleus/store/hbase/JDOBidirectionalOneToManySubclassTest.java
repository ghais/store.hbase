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
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.identity.StringIdentity;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.BidirectionalOneToManySubclassesJDO.Example1;
import org.datanucleus.test.models.BidirectionalOneToManySubclassesJDO.Example2;
import org.datanucleus.test.models.BidirectionalOneToManySubclassesJDO.Example3;
import org.datanucleus.test.models.BidirectionalOneToManySubclassesJDO.Example4;
import org.junit.Test;

/**
 */
public class JDOBidirectionalOneToManySubclassTest extends BaseTest
{

    @SuppressWarnings("unchecked")
    @Test
    public void testExample1Subclass() throws IOException, ClassNotFoundException
    {
        Example1.B parent = new Example1.B();
        parent.setAString("a string");
        parent.setBString("b string");

        Example1.X child = new Example1.X();
        child.setXString("x string");
        parent.getChildren().add(child);
        beginTxn();
        pm.get().makePersistent(parent);
        commitTxn();

        Result parentEntity = get("B", Bytes.toBytes(parent.getId()));
        Result childEntity = get("X", Bytes.toBytes(child.getId()));
        assertEquals(4, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "B", "aString"));
        assertEquals("b string", getStrValue(parentEntity, "B", "bString"));
        List<StringIdentity> childIds = deserialize(ArrayList.class, getBytes(parentEntity, "B", "children"));
        assertEquals(Utils.newArrayList(new StringIdentity(Example1.X.class, child.getId())), childIds);
        assertEquals(2, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "X", "xString"));

        beginTxn();
        parent = pm.get().getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals("b string", parent.getBString());
        assertEquals(1, parent.getChildren().size());
        assertEquals(child.getId(), parent.getChildren().get(0).getId());
        commitTxn();

        beginTxn();
        child = pm.get().getObjectById(child.getClass(), child.getId());
        assertEquals("x string", child.getXString());
        commitTxn();

        beginTxn();
        parent = pm.get().getObjectById(parent.getClass(), parent.getId());
        pm.get().deletePersistent(parent);
        commitTxn();
        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("B"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
        table = new HTable(Bytes.toBytes("X"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExample1Superclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();

        // insertion
        Example1.A parent = new Example1.A();
        parent.setAString("a string");

        Example1.X child = new Example1.X();
        child.setXString("x string");
        parent.getChildren().add(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();

        Result parentEntity = get("A", Bytes.toBytes(parent.getId()));
        Result childEntity = get("X", Bytes.toBytes(child.getId()));
        assertEquals(3, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "A", "aString"));
        List<StringIdentity> childIds = deserialize(ArrayList.class, getBytes(parentEntity, "A", "children"));
        assertEquals(Utils.newArrayList(new StringIdentity(Example1.X.class, child.getId())), childIds);
        assertEquals(2, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "X", "xString"));

        beginTxn();
        child = pm.getObjectById(child.getClass(), child.getId());
        assertEquals("x string", child.getXString());
        commitTxn();

        // cascade delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();
        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("A"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
        table = new HTable(Bytes.toBytes("X"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExample2Subclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example2.B parent = new Example2.B();
        parent.setAString("a string");
        parent.setBString("b string");

        Example2.Y child = new Example2.Y();
        child.setXString("x string");
        child.setYString("y string");
        parent.getChildren().add(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();

        Result parentEntity = get("B2", Bytes.toBytes(parent.getId()));
        Result childEntity = get("Y2", Bytes.toBytes(child.getId()));

        assertEquals(4, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "B2", "aString"));
        assertEquals("b string", getStrValue(parentEntity, "B2", "bString"));
        List<StringIdentity> childIds = deserialize(ArrayList.class, getBytes(parentEntity, "B2", "children"));
        assertEquals(Utils.newArrayList(new StringIdentity(Example2.Y.class, child.getId())), childIds);
        assertEquals(3, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "Y2", "xString"));
        assertEquals("y string", getStrValue(childEntity, "Y2", "yString"));

        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals("b string", parent.getBString());
        assertEquals(1, parent.getChildren().size());
        assertEquals(child.getId(), parent.getChildren().get(0).getId());
        commitTxn();

        beginTxn();
        child = pm.getObjectById(child.getClass(), child.getId());
        assertEquals("x string", child.getXString());
        assertEquals("y string", child.getYString());
        commitTxn();

        // cascade delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();

        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("B2"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
        table = new HTable(Bytes.toBytes("Y2"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();

    }

    @Test
    public void testExample2Superclass() throws IOException
    {
        PersistenceManager pm = BaseTest.pm.get();
        Example2.A parent = new Example2.A();
        parent.setAString("a string");

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();
        Result parentEntity = get("A2", Bytes.toBytes(parent.getId()));
        assertEquals(2, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "A2", "aString"));

        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        commitTxn();

        // delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();

        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("A2"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
    }

    @Test
    public void testExample3Superclass() throws IOException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example3.A parent = new Example3.A();
        parent.setAString("a string");

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();
        Result parentEntity = get("A3", Bytes.toBytes(parent.getId()));
        assertEquals(2, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "A3", "aString"));
        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        commitTxn();

        // delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();

        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("A3"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExample4Subclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example4.B parent = new Example4.B();
        parent.setAString("a string");
        parent.setBString("b string");

        Example4.Y child = new Example4.Y();
        child.setXString("x string");
        child.setYString("y string");
        parent.getChildren().add(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();

        Result parentEntity = get("B4", Bytes.toBytes(parent.getId()));
        Result childEntity = get("Y4", Bytes.toBytes(child.getId()));

        assertEquals(4, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "B4", "aString"));
        assertEquals("b string", getStrValue(parentEntity, "B4", "bString"));
        List<StringIdentity> childIds = deserialize(ArrayList.class, getBytes(parentEntity, "B4", "children"));
        assertEquals(Utils.newArrayList(new StringIdentity(Example4.Y.class, child.getId())), childIds);
        assertEquals(3, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "Y4", "xString"));
        assertEquals("y string", getStrValue(childEntity, "Y4", "yString"));

        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals("b string", parent.getBString());
        assertEquals(1, parent.getChildren().size());
        assertEquals(child.getId(), parent.getChildren().get(0).getId());
        commitTxn();

        beginTxn();
        child = pm.getObjectById(child.getClass(), child.getId());
        assertEquals("x string", child.getXString());
        assertEquals("y string", child.getYString());
        commitTxn();

        // cascade delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();

        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("B4"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
        table = new HTable(Bytes.toBytes("Y4"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
    }

    @Test
    public void testExample4Superclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example4.A parent = new Example4.A();
        parent.setAString("a string");

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();
        Result parentEntity = get("A4", Bytes.toBytes(parent.getId()));
        assertEquals(3, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "A4", "aString"));
        assertEquals(0, deserialize(ArrayList.class, getBytes(parentEntity, "A4", "children")).size());
        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        commitTxn();

        // delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();
        Scan scan = new Scan();
        HTable table = new HTable(Bytes.toBytes("A4"));
        assertFalse(table.getScanner(scan).iterator().hasNext());
        table.close();
    }

}
