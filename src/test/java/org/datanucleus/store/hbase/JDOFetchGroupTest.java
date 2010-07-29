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

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.HasFetchGroupsJDO;
import org.junit.Test;

/**
  */
public class JDOFetchGroupTest extends BaseTest
{
    @Test
    public void testDefaultFetchGroup()
    {
        PersistenceManager pm = getPersistenceManager();
        HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
        pojo.setStr1("1");
        pojo.setStr2("2");
        pojo.setStr3("3");
        pojo.setStr4("4");
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
        commitTxn();
        pm.close();
        assertEquals("1", pojo.getStr1());
        assertEquals("2", pojo.getStr2());
        assertNull(pojo.getStr3());
        assertEquals("4", pojo.getStr4());
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        pojo = pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId());
        pojo.getStr3();
        pojo = pm.detachCopy(pojo);
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        pojo = pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId());
        assertEquals("1", pojo.getStr1());
        assertEquals("2", pojo.getStr2());
        assertEquals("3", pojo.getStr3());
        assertEquals("4", pojo.getStr4());
        commitTxn();
    }

    @Test
    public void testCustomFetchGroup_ReplaceDefault()
    {
        PersistenceManager pm = getPersistenceManager();
        HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
        pojo.setStr1("1");
        pojo.setStr2("2");
        pojo.setStr3("3");
        pojo.setStr4("4");
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        pm.getFetchPlan().setGroup("fg1");
        pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        assertNull(pojo.getStr1());
        assertNull(pojo.getStr2());
        assertEquals("3", pojo.getStr3());
        assertNull(pojo.getStr4());
    }

    @Test
    public void testCustomFetchGroup_AddToDefault()
    {
        PersistenceManager pm = getPersistenceManager();
        HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
        pojo.setStr1("1");
        pojo.setStr2("2");
        pojo.setStr3("3");
        pojo.setStr4("4");
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        pm.getFetchPlan().addGroup("fg1");
        pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
        commitTxn();
        assertEquals("1", pojo.getStr1());
        assertEquals("2", pojo.getStr2());
        assertEquals("3", pojo.getStr3());
        assertEquals("4", pojo.getStr4());
    }

    @Test
    public void testFetchGroupWithQuery()
    {
        PersistenceManager pm = getPersistenceManager();
        HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
        pojo.setStr1("1");
        pojo.setStr2("2");
        pojo.setStr3("3");
        pojo.setStr4("4");
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        Query q = pm.newQuery(HasFetchGroupsJDO.class);
        q.setFilter("id == pojoId");
        q.declareParameters("String pojoId");
        q.setUnique(true);
        pojo = (HasFetchGroupsJDO) pm.detachCopy(q.execute(pojo.getId()));
        commitTxn();
        assertEquals("1", pojo.getStr1());
        assertEquals("2", pojo.getStr2());
        assertNull(pojo.getStr3());
        assertEquals("4", pojo.getStr4());
        beginTxn();
        pm.getFetchPlan().addGroup("fg1");
        pojo = (HasFetchGroupsJDO) pm.detachCopy(q.execute(pojo.getId()));
        commitTxn();
        pm.close();
        assertEquals("1", pojo.getStr1());
        assertEquals("2", pojo.getStr2());
        assertEquals("3", pojo.getStr3());
        assertEquals("4", pojo.getStr4());
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        q = pm.newQuery(HasFetchGroupsJDO.class);
        q.setFilter("id == pojoId");
        q.declareParameters("String pojoId");
        q.setUnique(true);
        pm.getFetchPlan().setGroup("fg1");
        pojo = (HasFetchGroupsJDO) pm.detachCopy(q.execute(pojo.getId()));
        commitTxn();
        assertNull(pojo.getStr1());
        assertNull(pojo.getStr2());
        assertEquals("3", pojo.getStr3());
        assertNull(pojo.getStr4());
    }

    @Test
    public void testFetchGroupOverridesCanBeManuallyUndone()
    {
        PersistenceManager pm = getPersistenceManager();
        HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
        pojo.setLink(new String("blarg"));
        makePersistentInTxn(pojo, TXN_START_END);
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        beginTxn();
        pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
        commitTxn();
        pm.close();
        BaseTest.pm.set(PMF.get().getPersistenceManager());
        pm = getPersistenceManager();
        assertNull(pojo.getLink());
    }
}
