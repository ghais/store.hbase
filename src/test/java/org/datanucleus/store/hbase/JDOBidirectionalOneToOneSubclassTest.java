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
import static org.junit.Assert.assertNull;

import java.io.IOException;

import javax.jdo.PersistenceManager;
import javax.jdo.identity.StringIdentity;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.BidirectionalOneToOneSubclassesJDO.Example1;
import org.datanucleus.test.models.BidirectionalOneToOneSubclassesJDO.Example2;
import org.datanucleus.test.models.BidirectionalOneToOneSubclassesJDO.Example3;
import org.junit.Test;

/**
 */
public class JDOBidirectionalOneToOneSubclassTest extends BaseTest
{
    @Test
    public void testExample1Subclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example1.B parent = new Example1.B();
        parent.setAString("a string");
        parent.setBString("b string");

        Example1.X child = new Example1.X();
        child.setXString("x string");
        parent.setChild(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();

        Result parentEntity = get("B1", Bytes.toBytes(parent.getId()));
        Result childEntity = get("X1", Bytes.toBytes(child.getId()));
        assertEquals(4, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "B1", "aString"));
        assertEquals("b string", getStrValue(parentEntity, "B1", "bString"));
        StringIdentity childId = deserialize(StringIdentity.class, getBytes(parentEntity, "B1", "child"));
        assertEquals(childId.getTargetClassName(), child.getClass().getName());
        assertEquals(childId.getKey(), child.getId());
        assertEquals(3, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "X1", "xString"));

        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals("b string", parent.getBString());
        assertEquals(child.getId(), parent.getChild().getId());
        commitTxn();

        beginTxn();
        child = pm.getObjectById(child.getClass(), child.getId());
        assertEquals("x string", child.getXString());
        commitTxn();

        // cascade delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();
        assertEquals(0, countForTable("B1"));
        assertEquals(0, countForTable("X1"));
    }

    @Test
    public void testExample1Superclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        Example1.A parent = new Example1.A();
        parent.setAString("a string");

        Example1.X child = new Example1.X();
        child.setXString("x string");
        parent.setChild(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();

        Result parentEntity = get("A1", Bytes.toBytes(parent.getId()));
        Result childEntity = get("X1", Bytes.toBytes(child.getId()));
        assertEquals(3, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "A1", "aString"));
        StringIdentity childId = deserialize(StringIdentity.class, getBytes(parentEntity, "A1", "child"));
        assertEquals(childId.getTargetClassName(), child.getClass().getName());
        assertEquals(childId.getKey(), child.getId());
        assertEquals(3, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "X1", "xString"));

        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals(child.getId(), parent.getChild().getId());
        commitTxn();

        beginTxn();
        child = pm.getObjectById(child.getClass(), child.getId());
        assertEquals("x string", child.getXString());
        commitTxn();

        // cascade delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();

        assertEquals(0, countForTable("A1"));
        assertEquals(0, countForTable("X1"));
    }

    @Test
    public void testExample3Subclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example2.B parent = new Example2.B();
        parent.setAString("a string");
        parent.setBString("b string");

        Example2.Y child = new Example2.Y();
        child.setXString("x string");
        child.setYString("y string");
        parent.setChild(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();

        Result parentEntity = get("B2", Bytes.toBytes(parent.getId()));
        Result childEntity = get("Y2", Bytes.toBytes(child.getId()));
        assertEquals(4, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "B2", "aString"));
        assertEquals("b string", getStrValue(parentEntity, "B2", "bString"));
        StringIdentity childId = deserialize(StringIdentity.class, getBytes(parentEntity, "B2", "child"));
        assertEquals(childId.getTargetClassName(), child.getClass().getName());
        assertEquals(childId.getKey(), child.getId());
        assertEquals(4, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "Y2", "xString"));
        assertEquals("y string", getStrValue(childEntity, "Y2", "yString"));
        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals("b string", parent.getBString());
        assertEquals(child.getId(), parent.getChild().getId());
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

        assertEquals(0, countForTable("B2"));
        assertEquals(0, countForTable("Y2"));
    }

    @Test
    public void testExample3Superclass() throws IOException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
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

        assertEquals(0, countForTable("A2"));
    }

    @Test
    public void testExample4Subclass() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = BaseTest.pm.get();
        // insertion
        Example3.B parent = new Example3.B();
        parent.setAString("a string");
        parent.setBString("b string");

        Example3.Y child = new Example3.Y();
        child.setXString("x string");
        child.setYString("y string");
        parent.setChild(child);

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();
        Result parentEntity = get("B3", Bytes.toBytes(parent.getId()));
        Result childEntity = get("Y3", Bytes.toBytes(child.getId()));

        assertEquals(4, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "B3", "aString"));
        assertEquals("b string", getStrValue(parentEntity, "B3", "bString"));
        StringIdentity childId = deserialize(StringIdentity.class, getBytes(parentEntity, "B3", "child"));
        assertEquals(childId.getTargetClassName(), child.getClass().getName());
        assertEquals(childId.getKey(), child.getId());
        assertEquals(4, childEntity.size());
        assertEquals("x string", getStrValue(childEntity, "Y3", "xString"));
        assertEquals("y string", getStrValue(childEntity, "Y3", "yString"));

        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        assertEquals("b string", parent.getBString());
        assertEquals(child.getId(), parent.getChild().getId());
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

        assertEquals(0, countForTable("B3"));
        assertEquals(0, countForTable("Y3"));
    }

    @Test
    public void testExample4Superclass() throws IOException
    {
        PersistenceManager pm = BaseTest.pm.get();
        Example3.A parent = new Example3.A();
        parent.setAString("a string");

        beginTxn();
        pm.makePersistent(parent);
        commitTxn();
        Result parentEntity = get("A3", Bytes.toBytes(parent.getId()));
        assertEquals(2, parentEntity.size());
        assertEquals("a string", getStrValue(parentEntity, "A3", "aString"));
        assertNull(getBytes(parentEntity, "A3", "child"));
        // lookup
        beginTxn();
        parent = pm.getObjectById(parent.getClass(), parent.getId());
        assertEquals("a string", parent.getAString());
        commitTxn();

        // delete
        beginTxn();
        pm.deletePersistent(parent);
        commitTxn();
        assertEquals(0, countForTable("A3"));
    }

}
