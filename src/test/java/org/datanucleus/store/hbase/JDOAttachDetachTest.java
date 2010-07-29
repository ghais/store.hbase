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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;

import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.DetachableJDO;
import org.junit.Test;

/**
 */
public class JDOAttachDetachTest extends BaseTest
{

    @SuppressWarnings("unchecked")
    private <T extends Serializable> T toBytesAndBack(T obj) throws IOException, ClassNotFoundException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (T) ois.readObject();
    }

    @Test
    public void testSimpleSerializeWithTxns() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = null;
        DetachableJDO pojo = new DetachableJDO();
        Date now = new Date();
        pm = PMF.get().getPersistenceManager();
        pm.setDetachAllOnCommit(true);

        try
        {
            Transaction tx = pm.currentTransaction();
            tx.begin();

            pojo.setVal("yar");
            pojo.setDate(now);
            pm.makePersistent(pojo);
            tx.commit();
            assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
            assertEquals(Date.class, pojo.getDate().getClass());
            pm.close();
        }
        finally
        {
            pm.close();
        }
        assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
        pm = PMF.get().getPersistenceManager();
        assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));

        pojo = toBytesAndBack(pojo);

        assertEquals("yar", pojo.getVal());
        assertEquals(now, pojo.getDate());
        assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
        pm = PMF.get().getPersistenceManager();
        Date newDate = new Date(pojo.getDate().getTime() + 1);
        try
        {
            assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(pojo));
            pojo.setVal("not yar");
            pojo.getDate().setTime(newDate.getTime());
            assertEquals(ObjectState.DETACHED_DIRTY, JDOHelper.getObjectState(pojo));
            pm.makePersistent(pojo);
        }
        finally
        {
            pm.close();
        }
    }
}
