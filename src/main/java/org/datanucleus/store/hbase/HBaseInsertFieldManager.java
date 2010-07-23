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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

public class HBaseInsertFieldManager extends AbstractFieldManager
{
    Put put;

    Delete delete;

    AbstractClassMetaData acmd;

    ObjectProvider objectProvider;

    public HBaseInsertFieldManager(AbstractClassMetaData acmd, ObjectProvider objectProvider, Put put, Delete delete)
    {
        this.acmd = acmd;
        this.put = put;
        this.delete = delete;
        this.objectProvider = objectProvider;
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);

        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));

    }

    public void storeByteField(int fieldNumber, byte value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeCharField(int fieldNumber, char value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeIntField(int fieldNumber, int value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeLongField(int fieldNumber, long value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        if (value == null)
        {
            delete.deleteColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        }
        else
        {
            ExecutionContext context = objectProvider.getExecutionContext();
            ClassLoaderResolver clr = context.getClassLoaderResolver();
            AbstractMemberMetaData fieldMetaData = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            int relationType = fieldMetaData.getRelationType(clr);

            switch (relationType)
            {
                case Relation.ONE_TO_ONE_BI :
                case Relation.ONE_TO_ONE_UNI :
                case Relation.MANY_TO_ONE_BI :
                {
                    Object persisted = context.persistObjectInternal(value, objectProvider, -1, StateManager.PC);

                    Object valueId = context.getApiAdapter().getIdForObject(persisted);

                    try
                    {

                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(valueId);

                        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), bos.toByteArray());

                        oos.close();
                        bos.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                    break;
                }
                case Relation.MANY_TO_MANY_BI :
                case Relation.ONE_TO_MANY_BI :
                case Relation.ONE_TO_MANY_UNI :
                {
                    if (value instanceof Collection)
                    {

                        List<Object> mapping = new ArrayList<Object>();

                        for (Object c : (Collection) value)
                        {

                            Object persisted = context.persistObjectInternal(c, objectProvider, -1, StateManager.PC);
                            Object valueId = context.getApiAdapter().getIdForObject(persisted);
                            mapping.add(valueId);
                        }

                        try
                        {

                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(mapping);

                            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), bos.toByteArray());

                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else if (value instanceof Map)
                    {
                        // Process all keys, values of the Map that are PC

                        Map<Object, Object> mapping = new TreeMap<Object, Object>();

                        Map map = (Map) value;
                        ApiAdapter api = context.getApiAdapter();
                        Set keys = map.keySet();
                        Iterator iter = keys.iterator();
                        while (iter.hasNext())
                        {
                            Object mapKey = iter.next();
                            Object key = null;

                            if (api.isPersistable(mapKey))
                            {
                                Object persisted = context.persistObjectInternal(mapKey, objectProvider, -1, StateManager.PC);
                                key = context.getApiAdapter().getIdForObject(persisted);
                            }
                            else
                            {
                                key = mapKey;
                            }

                            Object mapValue = map.get(key);
                            Object key_value = null;

                            if (api.isPersistable(mapValue))
                            {

                                Object persisted = context.persistObjectInternal(mapValue, objectProvider, -1, StateManager.PC);
                                key_value = context.getApiAdapter().getIdForObject(persisted);
                            }
                            else
                            {
                                key_value = mapValue;
                            }

                            mapping.put(key, key_value);

                        }

                        try
                        {

                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(mapping);

                            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), bos.toByteArray());

                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    break;
                }

                default :
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(value);

                        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), bos.toByteArray());

                        oos.close();
                        bos.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                    break;
            }
        }
    }

    public void storeShortField(int fieldNumber, short value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

    public void storeStringField(int fieldNumber, String value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        if (value == null)
        {
            delete.deleteColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        }
        else
        {
            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        }
    }
}
