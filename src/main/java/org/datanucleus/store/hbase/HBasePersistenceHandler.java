/**********************************************************************
Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
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
package org.datanucleus.store.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.util.Localiser;

public class HBasePersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance("Localisation", HBaseStoreManager.class.getClassLoader());

    protected final HBaseStoreManager storeMgr;

    HBasePersistenceHandler(StoreManager storeMgr)
    {
        this.storeMgr = (HBaseStoreManager) storeMgr;
    }

    public void close()
    {
        // TODO Auto-generated method stub

    }

    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));

            table.delete(newDelete(sm));
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void fetchObject(ObjectProvider sm, int[] fieldNumbers)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            Result result = getResult(sm, table);
            if (result.getRow() == null)
            {
                throw new NucleusObjectNotFoundException();
            }
            HBaseFetchFieldManager fm = new HBaseFetchFieldManager(acmd, sm, result);
            sm.replaceFields(acmd.getAllMemberPositions(), fm);
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public Object findObject(ExecutionContext ectx, Object id)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void insertObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        if (!storeMgr.managesClass(sm.getClassMetaData().getFullClassName()))
        {
            storeMgr.addClass(sm.getClassMetaData().getFullClassName(), sm.getExecutionContext().getClassLoaderResolver());
        }
        // Check existence of the object since HBase doesn't enforce application identity
        try
        {
            locateObject(sm);
            throw new NucleusUserException(LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists", sm.toPrintableID(),
                sm.getInternalObjectId()));
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            // Do nothing since object with this id doesn't exist
        }

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            Put put = newPut(sm);
            Delete delete = newDelete(sm);
            HBaseInsertFieldManager fm = new HBaseInsertFieldManager(acmd, sm, put, delete);
            sm.provideFields(acmd.getAllMemberPositions(), fm);
            table.put(put);
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    private Put newPut(ObjectProvider sm) throws IOException
    {
        byte[] pk = HBaseUtils.getPrimaryKeyBytes(sm);
        Put batch = new Put(pk);
        return batch;
    }

    private Delete newDelete(ObjectProvider sm) throws IOException
    {

        byte[] pk = HBaseUtils.getPrimaryKeyBytes(sm);
        Delete batch = new Delete(pk);
        return batch;
    }

    private Result getResult(ObjectProvider sm, HTable table) throws IOException
    {
        byte[] pk = HBaseUtils.getPrimaryKeyBytes(sm);
        Get get = new Get(pk);
        Result result = table.get(get);

        return result;
    }

    private boolean exists(ObjectProvider sm, HTable table) throws IOException
    {
        byte[] pk = HBaseUtils.getPrimaryKeyBytes(sm);
        Get get = new Get(pk);
        boolean result = table.exists(get);
        return result;
    }

    public void locateObject(ObjectProvider sm)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            if (!exists(sm, table))
            {
                throw new NucleusObjectNotFoundException();
            }
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void updateObject(ObjectProvider sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            Put put = newPut(sm);
            Delete delete = newDelete(sm); // we will ignore the delete object
            HBaseInsertFieldManager fm = new HBaseInsertFieldManager(acmd, sm, put, delete);
            sm.provideFields(fieldNumbers, fm);
            if (!put.isEmpty())
            {
                table.put(put);
            }
            if (!delete.isEmpty())
            {
                // only delete if there are columns to delete. Otherwise an empty delete would cause the
                // entire row to be deleted
                table.delete(delete);
            }
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }
}