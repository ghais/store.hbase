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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Result;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.types.sco.backed.Vector;

public class HBaseFetchFieldManager extends AbstractFieldManager
{
    Result result;

    AbstractClassMetaData acmd;

    ObjectProvider objectProvider;

    public HBaseFetchFieldManager(AbstractClassMetaData acmd, ObjectProvider objectProvider, Result result)
    {
        this.acmd = acmd;
        this.result = result;
        this.objectProvider = objectProvider;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        boolean value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readBoolean();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public byte fetchByteField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        byte value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readByte();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public char fetchCharField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        char value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readChar();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public double fetchDoubleField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        double value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readDouble();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public float fetchFloatField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        float value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readFloat();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public int fetchIntField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        int value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readInt();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public long fetchLongField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        long value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readLong();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public Object fetchObjectField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);

        ExecutionContext context = objectProvider.getExecutionContext();
        ClassLoaderResolver clr = context.getClassLoaderResolver();
        AbstractMemberMetaData fieldMetaData = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

        // get object
        Object value;
        try
        {
            try
            {
                byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readObject();
                ois.close();
                bis.close();
            }
            catch (NullPointerException ex)
            {
                return null;
            }
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }

        // handle relations
        int relationType = fieldMetaData.getRelationType(clr);

        switch (relationType)
        {
            case Relation.ONE_TO_ONE_BI :
            case Relation.ONE_TO_ONE_UNI :
            case Relation.MANY_TO_ONE_BI :
            {
                Object id = value;
                String class_name = fieldMetaData.getClassName();
                value = context.findObject(id, true, false, class_name);
                break;
            }
            case Relation.MANY_TO_MANY_BI :
            case Relation.ONE_TO_MANY_BI :
            case Relation.ONE_TO_MANY_UNI :
            {
                MetaDataManager mmgr = context.getMetaDataManager();

                if (fieldMetaData.hasCollection())
                {

                    String elementClassName = fieldMetaData.getCollection().getElementType();

                    List<Object> mapping = (List<Object>) value;
                    Collection<Object> collection = null;
                    if (TreeSet.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new TreeSet<Object>();
                    }
                    else if (LinkedHashSet.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new LinkedHashSet<Object>();
                    }
                    else if (HashSet.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new HashSet<Object>();
                    }
                    else if (SortedSet.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new TreeSet<Object>();
                    }
                    else if (Set.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new TreeSet<Object>();
                    }
                    else if (ArrayList.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new ArrayList<Object>();
                    }
                    else if (LinkedList.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new LinkedList<Object>();
                    }
                    else if (Vector.class.isAssignableFrom(fieldMetaData.getType()))
                    {
                        collection = new java.util.Vector<Object>();
                    }
                    for (Object id : mapping)
                    {

                        Object element = context.findObject(id, true, false, elementClassName);
                        collection.add(element);
                    }
                    value = collection;
                }

                else if (fieldMetaData.hasMap())
                {
                    // Process all keys, values of the Map that are PC

                    String key_elementClassName = fieldMetaData.getMap().getKeyType();
                    String value_elementClassName = fieldMetaData.getMap().getValueType();

                    Map<Object, Object> mapping = new TreeMap<Object, Object>();

                    Map map = (Map) value;
                    ApiAdapter api = context.getApiAdapter();

                    Set keys = map.keySet();
                    Iterator iter = keys.iterator();
                    while (iter.hasNext())
                    {
                        Object mapKey = iter.next();
                        Object key = null;

                        if (mapKey instanceof javax.jdo.identity.SingleFieldIdentity)
                        {
                            key = context.findObject(mapKey, true, false, key_elementClassName);

                        }
                        else
                        {
                            key = mapKey;
                        }

                        Object mapValue = map.get(key);
                        Object key_value = null;

                        if (mapValue instanceof javax.jdo.identity.SingleFieldIdentity)
                        {

                            key_value = context.findObject(mapValue, true, false, value_elementClassName);
                        }
                        else
                        {
                            key_value = mapValue;
                        }

                        mapping.put(key, key_value);
                    }

                    value = mapping;
                }
                break;
            }

            default :
                break;
        }
        return value;

    }

    public short fetchShortField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        short value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readShort();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }

    public String fetchStringField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        String value;
        try
        {
            try
            {
                byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = (String) ois.readObject();
                ois.close();
                bis.close();
            }
            catch (NullPointerException ex)
            {
                return null;
            }
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
}
