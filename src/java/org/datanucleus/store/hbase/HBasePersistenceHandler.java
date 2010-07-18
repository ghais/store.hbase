/**********************************************************************
 * Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Contributors :
 * ...
 ***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

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

public class HBasePersistenceHandler extends AbstractPersistenceHandler {

    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
            "org.datanucleus.store.hbase.Localisation",
            HBaseStoreManager.class.getClassLoader());

    protected final HBaseStoreManager storeMgr;

    HBasePersistenceHandler(final StoreManager storeMgr) {
        this.storeMgr = (HBaseStoreManager) storeMgr;
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public void deleteObject(final ObjectProvider sm) {
        // Check if read-only so update not permitted
        this.storeMgr.assertReadOnlyForUpdateOfObject(sm);

        final HBaseManagedConnection mconn = (HBaseManagedConnection) this.storeMgr
                .getConnection(sm.getExecutionContext());
        try {
            final AbstractClassMetaData acmd = sm.getClassMetaData();
            final HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));

            table.delete(this.newDelete(sm));
        } catch (final IOException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            mconn.release();
        }
    }

    public void fetchObject(final ObjectProvider sm, final int[] fieldNumbers) {
        final HBaseManagedConnection mconn = (HBaseManagedConnection) this.storeMgr
                .getConnection(sm.getExecutionContext());
        try {
            final AbstractClassMetaData acmd = sm.getClassMetaData();
            final HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            final Result result = this.getResult(sm, table);
            if (result.getRow() == null) {
                throw new NucleusObjectNotFoundException();
            }
            final HBaseFetchFieldManager fm = new HBaseFetchFieldManager(acmd,
                    result);
            sm.replaceFields(acmd.getAllMemberPositions(), fm);
            table.close();
        } catch (final IOException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            mconn.release();
        }
    }

    public Object findObject(final ExecutionContext ectx, final Object id) {
        // TODO Auto-generated method stub
        return null;
    }

    public void insertObject(final ObjectProvider sm) {
        // Check if read-only so update not permitted
        this.storeMgr.assertReadOnlyForUpdateOfObject(sm);

        if (!this.storeMgr.managesClass(sm.getClassMetaData()
                .getFullClassName())) {
            this.storeMgr.addClass(sm.getClassMetaData().getFullClassName(), sm
                    .getExecutionContext().getClassLoaderResolver());
        }
        // Check existence of the object since HBase doesn't enforce application
        // identity
        try {
            this.locateObject(sm);
            throw new NucleusUserException(
                    LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists"));
            // TODO add JVM ID of object
            // throw new
            // NucleusUserException(LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists",
            // sm.toPrintableID(), sm.getInternalObjectId()));
        } catch (final NucleusObjectNotFoundException onfe) {
            // Do nothing since object with this id doesn't exist
        }

        final HBaseManagedConnection mconn = (HBaseManagedConnection) this.storeMgr
                .getConnection(sm.getExecutionContext());
        try {
            final AbstractClassMetaData acmd = sm.getClassMetaData();
            final HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            final Put put = this.newPut(sm);
            final Delete delete = this.newDelete(sm);
            final HBaseInsertFieldManager fm = new HBaseInsertFieldManager(
                    acmd, put, delete);
            sm.provideFields(acmd.getAllMemberPositions(), fm);
            table.put(put);
            table.close();
        } catch (final IOException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            mconn.release();
        }
    }

    public void locateObject(final ObjectProvider sm) {
        final HBaseManagedConnection mconn = (HBaseManagedConnection) this.storeMgr
                .getConnection(sm.getExecutionContext());
        try {
            final AbstractClassMetaData acmd = sm.getClassMetaData();
            final HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            if (!this.exists(sm, table)) {
                throw new NucleusObjectNotFoundException();
            }
            table.close();
        } catch (final IOException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            mconn.release();
        }
    }

    public void updateObject(final ObjectProvider sm, final int[] fieldNumbers) {
        // Check if read-only so update not permitted
        this.storeMgr.assertReadOnlyForUpdateOfObject(sm);

        final HBaseManagedConnection mconn = (HBaseManagedConnection) this.storeMgr
                .getConnection(sm.getExecutionContext());
        try {
            final AbstractClassMetaData acmd = sm.getClassMetaData();
            final HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            final Put put = this.newPut(sm);
            final Delete delete = this.newDelete(sm); // we will ignore the
                                                      // delete
            // object
            final HBaseInsertFieldManager fm = new HBaseInsertFieldManager(
                    acmd, put, delete);
            sm.provideFields(fieldNumbers, fm);
            if (!put.isEmpty()) {
                table.put(put);
            }
            if (!delete.isEmpty()) {
                // only delete if there are columns to delete. Otherwise an
                // empty delete would cause the
                // entire row to be deleted
                table.delete(delete);
            }
            table.close();
        } catch (final IOException e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        } finally {
            mconn.release();
        }
    }

    private boolean exists(final ObjectProvider sm, final HTable table)
            throws IOException {
        final Object pkValue = sm.provideField(sm.getClassMetaData()
                .getPKMemberPositions()[0]);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        final Get get = new Get(bos.toByteArray());
        final boolean result = table.exists(get);
        oos.close();
        bos.close();
        return result;
    }

    private Result getResult(final ObjectProvider sm, final HTable table)
            throws IOException {
        final Object pkValue = sm.provideField(sm.getClassMetaData()
                .getPKMemberPositions()[0]);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        final Get get = new Get(bos.toByteArray());
        final Result result = table.get(get);
        oos.close();
        bos.close();
        return result;
    }

    private Delete newDelete(final ObjectProvider sm) throws IOException {
        final Object pkValue = sm.provideField(sm.getClassMetaData()
                .getPKMemberPositions()[0]);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        final Delete batch = new Delete(bos.toByteArray());
        oos.close();
        bos.close();
        return batch;
    }

    private Put newPut(final ObjectProvider sm) throws IOException {
        final Object pkValue = sm.provideField(sm.getClassMetaData()
                .getPKMemberPositions()[0]);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        final Put batch = new Put(bos.toByteArray());
        oos.close();
        bos.close();
        return batch;
    }
}
