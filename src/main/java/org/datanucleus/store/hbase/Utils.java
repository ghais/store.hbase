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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.ObjectProvider;

public class Utils
{
    public static String getTableName(AbstractClassMetaData acmd)
    {
        if (acmd.getTable() != null)
        {
            return acmd.getTable();
        }

        return acmd.getName();
    }

    public static String getFamilyName(AbstractClassMetaData acmd, int absoluteFieldNumber)
    {
        AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData[] colmds = ammd.getColumnMetaData();
        if (colmds != null && colmds.length > 0)
        {
            columnName = colmds[0].getName();
        }
        if (columnName == null)
        {
            // Fallback to the field/property name
            columnName = Utils.getTableName(acmd);
        }
        else if (columnName.indexOf(":") > -1)
        {
            columnName = columnName.substring(0, columnName.indexOf(":"));
        }
        return columnName;
    }

    public static String getQualifierName(AbstractClassMetaData acmd, int absoluteFieldNumber)
    {
        AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData[] colmds = ammd.getColumnMetaData();
        if (colmds != null && colmds.length > 0)
        {
            columnName = colmds[0].getName();
        }
        if (columnName == null)
        {
            // Fallback to the field/property name
            columnName = ammd.getName();
        }
        if (columnName.indexOf(":") > -1)
        {
            columnName = columnName.substring(columnName.indexOf(":") + 1);
        }
        return columnName;
    }

    /**
     * Create a schema in HBase. Do not make this method public, since it uses privileged actions
     * @param config
     * @param acmd
     * @param autoCreateColumns
     */
    @SuppressWarnings("unchecked")
    static void createSchema(final HBaseConfiguration config, final AbstractClassMetaData acmd, final boolean autoCreateColumns)
    {

        try
        {
            final HBaseAdmin hBaseAdmin = (HBaseAdmin) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    return new HBaseAdmin(config);
                }
            });
            final HTableDescriptor hTable = (HTableDescriptor) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {

                public Object run() throws Exception
                {
                    String tableName = Utils.getTableName(acmd);
                    HTableDescriptor hTable;
                    try
                    {
                        hTable = hBaseAdmin.getTableDescriptor(tableName.getBytes());
                    }
                    catch (TableNotFoundException ex)
                    {
                        hTable = new HTableDescriptor(tableName);
                        hBaseAdmin.createTable(hTable);
                    }
                    return hTable;
                }
            });

            if (autoCreateColumns)
            {
                boolean modified = false;
                if (!hTable.hasFamily(Utils.getTableName(acmd).getBytes()))
                {
                    HColumnDescriptor hColumn = new HColumnDescriptor(Utils.getTableName(acmd));
                    hTable.addFamily(hColumn);
                    modified = true;
                }
                int[] fieldNumbers = acmd.getAllMemberPositions();
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    String familyName = getFamilyName(acmd, fieldNumbers[i]);
                    if (!hTable.hasFamily(familyName.getBytes()))
                    {
                        HColumnDescriptor hColumn = new HColumnDescriptor(familyName);
                        hTable.addFamily(hColumn);
                        modified = true;
                    }
                }
                if (modified)
                {
                    AccessController.doPrivileged(new PrivilegedExceptionAction()
                    {
                        public Object run() throws Exception
                        {
                            hBaseAdmin.disableTable(hTable.getName());
                            hBaseAdmin.modifyTable(hTable.getName(), hTable);
                            hBaseAdmin.enableTable(hTable.getName());
                            return null;
                        }
                    });
                }
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
    }

    static byte[] getPrimaryKeyBytes(ObjectProvider sm) throws IOException
    {
        AbstractClassMetaData acmd = sm.getClassMetaData();
        Class<?> type = acmd.getMetaDataForManagedMemberAtAbsolutePosition(sm.getClassMetaData().getPKMemberPositions()[0]).getType();
        Object pkValue = sm.provideField(acmd.getPKMemberPositions()[0]);
        if (pkValue == null)
        {
            return null;
        }
        if (type.equals(String.class))
        {
            return Bytes.toBytes((String) pkValue);
        }
        else if (type.equals(Long.class) || type.equals(Long.TYPE))
        {
            return Bytes.toBytes((Long) pkValue);
        }
        else
        {
            ByteArrayOutputStream bos = null;
            ObjectOutputStream oos = null;
            try
            {
                bos = new ByteArrayOutputStream();
                oos = new ObjectOutputStream(bos);
                oos.writeObject(pkValue);
                return bos.toByteArray();

            }
            finally
            {
                oos.close();
                bos.close();
            }
        }
    }

    public static <T> ArrayList<T> newArrayList(T... elements)
    {
        return new ArrayList<T>(Arrays.asList(elements));
    }

    public static <T> LinkedList<T> newLinkedList(T... elements)
    {
        return new LinkedList<T>(Arrays.asList(elements));
    }

    public static <T> HashSet<T> newHashSet(T... elements)
    {
        return new HashSet<T>(Arrays.asList(elements));
    }

    public static <T> HashSet<T> newHashSet(Collection<T> elements)
    {
        return new HashSet<T>(elements);
    }

    public static <T> TreeSet<T> newTreeSet(T... elements)
    {
        return new TreeSet<T>(Arrays.asList(elements));
    }

    public static <F, T> List<T> transform(Iterable<F> from, Function<? super F, ? extends T> func)
    {
        List<T> to = new ArrayList<T>();
        for (F fromElement : from)
        {
            to.add(func.apply(fromElement));
        }
        return to;
    }

    public static <T> Function<T, T> identity()
    {
        return new Function<T, T>()
        {
            public T apply(T from)
            {
                return from;
            }
        };
    }

    public static <K, V> HashMap<K, V> newHashMap()
    {
        return new HashMap<K, V>();
    }

    public static <K, V> TreeMap<K, V> newTreehMap()
    {
        return new TreeMap<K, V>();
    }

    public static <T> LinkedHashSet<T> newLinkedHashSet(T... elements)
    {
        return new LinkedHashSet<T>(Arrays.asList(elements));
    }

    public interface Function<F, T>
    {
        T apply(F from);
    }

    public static byte[] serializeObject(Object o) throws IOException
    {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try
        {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            return bos.toByteArray();
        }
        finally
        {
            oos.close();
            bos.close();
        }

    }

}
