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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.KitchenSink;
import org.junit.Test;

/**
 */
public class JDODeleteTest extends BaseTest
{

    @Test
    public void testSimpleDelete()
    {
        PersistenceManager pm = getPersistenceManager();
        String keyStr = "key";

        beginTxn();
        pm.makePersistent(KitchenSink.newKitchenSink(keyStr));
        commitTxn();
        beginTxn();
        KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
        assertNotNull(ks);
        pm.deletePersistent(ks);
        commitTxn();
        beginTxn();
        try
        {
            pm.getObjectById(KitchenSink.class, keyStr);
            fail("expected onfe");
        }
        catch (JDOObjectNotFoundException onfe)
        {
            // good
        }
        finally
        {
            rollbackTxn();
        }
    }

    @Test
    public void testNonTransactionalDelete()
    {
        PersistenceManager pm = getPersistenceManager();
        String keyStr = "key";

        beginTxn();
        pm.makePersistent(KitchenSink.newKitchenSink(keyStr));
        commitTxn();
        KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
        assertNotNull(ks);
        pm.deletePersistent(ks);
        try
        {
            pm.getObjectById(KitchenSink.class, keyStr);
            fail("expected onfe");
        }
        catch (JDOObjectNotFoundException onfe)
        {
            // good
        }
    }

    @Test
    public void testDeletePersistentNew()
    {
        PersistenceManager pm = getPersistenceManager();
        beginTxn();
        KitchenSink ks = KitchenSink.newKitchenSink("key");
        pm.makePersistent(ks);
        String keyStr = ks.key;
        pm.deletePersistent(ks);
        commitTxn();
        beginTxn();
        try
        {
            pm.getObjectById(KitchenSink.class, keyStr);
            fail("expected onfe");
        }
        catch (JDOObjectNotFoundException onfe)
        {
            // good
        }
        finally
        {
            rollbackTxn();
        }
    }

    @Test
    public void testDeletePersistentNew_NoTxn()
    {
        PersistenceManager pm = getPersistenceManager();
        KitchenSink ks = KitchenSink.newKitchenSink("key");
        pm.makePersistent(ks);
        String keyStr = ks.key;
        pm.deletePersistent(ks);
        pm.close();
        pm = PMF.get().getPersistenceManager();
        try
        {
            pm.getObjectById(KitchenSink.class, keyStr);
            fail("expected onfe");
        }
        catch (JDOObjectNotFoundException onfe)
        {
            // good
        }
    }

}
