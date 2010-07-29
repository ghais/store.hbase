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
package org.datanucleus.test.models;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.store.hbase.Utils;

/**
 * A class that contains members of all the types we know how to map.
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(detachable = "true")
public class KitchenSink implements Serializable
{

    public KitchenSink()
    {
    }

    public enum KitchenSinkEnum {
        ONE, TWO
    }

    public static final Date DATE1 = new Date(147);

    public static final Date DATE2 = new Date(247);

    @PrimaryKey
    @Persistent
    public String key;

    @Persistent
    public String strVal;

    @Persistent
    public Boolean boolVal;

    @Persistent
    public boolean boolPrimVal;

    @Persistent
    public Long longVal;

    @Persistent
    public long longPrimVal;

    @Persistent
    public Integer integerVal;

    @Persistent
    public int intVal;

    @Persistent
    public Character characterVal;

    @Persistent
    public char charVal;

    @Persistent
    public Short shortVal;

    @Persistent
    public short shortPrimVal;

    @Persistent
    public Byte byteVal;

    @Persistent
    public byte bytePrimVal;

    @Persistent
    public Float floatVal;

    @Persistent
    public float floatPrimVal;

    @Persistent
    public Double doubleVal;

    @Persistent
    public double doublePrimVal;

    @Persistent
    public Date dateVal;

    @Persistent
    public KitchenSinkEnum ksEnum;

    @Persistent
    public BigDecimal bigDecimal;

    @Persistent
    public String[] strArray;

    @Persistent
    public int[] primitiveIntArray;

    @Persistent
    public Integer[] integerArray;

    @Persistent
    public long[] primitiveLongArray;

    @Persistent
    public Long[] longArray;

    @Persistent
    public short[] primitiveShortArray;

    @Persistent
    public Short[] shortArray;

    @Persistent
    public char[] primitiveCharArray;

    @Persistent
    public Character[] characterArray;

    @Persistent
    public float[] primitiveFloatArray;

    @Persistent
    public Float[] floatArray;

    @Persistent
    public double[] primitiveDoubleArray;

    @Persistent
    public Double[] doubleArray;

    @Persistent
    public byte[] primitiveByteArray;

    @Persistent
    public Byte[] byteArray;

    @Persistent
    public boolean[] primitiveBooleanArray;

    @Persistent
    public Boolean[] booleanArray;

    @Persistent
    public Date[] dateArray;

    @Persistent
    public KitchenSinkEnum[] ksEnumArray;

    @Persistent(defaultFetchGroup = "true")
    public BigDecimal[] bigDecimalArray;

    @Persistent
    public List<String> strList;

    @Persistent
    public List<Integer> integerList;

    @Persistent
    public List<Long> longList;

    @Persistent
    public List<Short> shortList;

    @Persistent
    public List<Character> charList;

    @Persistent
    public List<Byte> byteList;

    @Persistent
    public List<Double> doubleList;

    @Persistent
    public List<Float> floatList;

    @Persistent
    public List<Boolean> booleanList;

    @Persistent
    public List<Date> dateList;

    @Persistent
    public List<KitchenSinkEnum> ksEnumList;

    @Persistent(defaultFetchGroup = "true")
    public List<BigDecimal> bigDecimalList;

    public static KitchenSink newKitchenSink()
    {
        return newKitchenSink(UUID.randomUUID().toString());
    }

    public static KitchenSink newKitchenSink(String key)
    {
        KitchenSink ks = new KitchenSink();
        ks.key = key;
        ks.strVal = "strVal";
        ks.boolVal = true;
        ks.boolPrimVal = true;
        ks.longVal = 4L;
        ks.longPrimVal = 4L;
        ks.integerVal = 3;
        ks.intVal = 3;
        ks.characterVal = 'a';
        ks.charVal = 'a';
        ks.shortVal = (short) 2;
        ks.shortPrimVal = (short) 2;
        ks.byteVal = 0xb;
        ks.bytePrimVal = 0xb;
        ks.floatVal = 1.01f;
        ks.floatPrimVal = 1.01f;
        ks.doubleVal = 2.22d;
        ks.doublePrimVal = 2.22d;
        ks.dateVal = DATE1;
        ks.ksEnum = KitchenSinkEnum.ONE;
        ks.bigDecimal = new BigDecimal(2);

        ks.strArray = new String[]{"a", "b"};
        ks.primitiveIntArray = new int[]{1, 2};
        ks.integerArray = new Integer[]{3, 4};
        ks.primitiveLongArray = new long[]{5L, 6L};
        ks.longArray = new Long[]{7L, 8L};
        ks.primitiveShortArray = new short[]{(short) 9, (short) 10};
        ks.shortArray = new Short[]{(short) 11, (short) 12};
        ks.primitiveCharArray = new char[]{'a', 'b'};
        ks.characterArray = new Character[]{'c', 'd'};
        ks.primitiveFloatArray = new float[]{1.01f, 1.02f};
        ks.floatArray = new Float[]{1.03f, 1.04f};
        ks.primitiveDoubleArray = new double[]{2.01d, 2.02d};
        ks.doubleArray = new Double[]{2.03d, 2.04d};
        ks.primitiveByteArray = new byte[]{0xb, 0xc};
        ks.byteArray = new Byte[]{0xe, 0xf};
        ks.primitiveBooleanArray = new boolean[]{true, false};
        ks.booleanArray = new Boolean[]{Boolean.FALSE, Boolean.TRUE};
        ks.dateArray = new Date[]{DATE1, DATE2};
        ks.ksEnumArray = new KitchenSinkEnum[]{KitchenSinkEnum.TWO, KitchenSinkEnum.ONE};
        ks.bigDecimalArray = new BigDecimal[]{new BigDecimal(3), new BigDecimal(4)};

        ks.strList = Utils.newArrayList("p", "q");
        ks.integerList = Utils.newArrayList(11, 12);
        ks.longList = Utils.newArrayList(13L, 14L);
        ks.shortList = Utils.newArrayList((short) 15, (short) 16);
        ks.charList = Utils.newArrayList('q', 'r');
        ks.byteList = Utils.newArrayList((byte) 0x8, (byte) 0x9);
        ks.doubleList = Utils.newArrayList(22.44d, 23.55d);
        ks.floatList = Utils.newArrayList(23.44f, 24.55f);
        ks.booleanList = Utils.newArrayList(true, false);
        ks.dateList = Utils.newArrayList(DATE1, DATE2);
        ks.ksEnumList = Utils.newArrayList(KitchenSinkEnum.TWO, KitchenSinkEnum.ONE);
        ks.bigDecimalList = Utils.newArrayList(new BigDecimal(7), new BigDecimal(6));
        return ks;
    }

    public static Put newKitchenSinkPut(String key) throws IOException
    {
        Put put = new Put(Bytes.toBytes(key));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("key"), Bytes.toBytes(key));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("strVal"), Bytes.toBytes("strVal"));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("boolVal"), Utils.serializeObject(Boolean.TRUE));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("boolPrimVal"), Bytes.toBytes(true));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("longVal"), Utils.serializeObject(new Long(4L)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("longPrimVal"), Bytes.toBytes(4L));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("integerVal"), Utils.serializeObject(new Integer(3)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("intVal"), Bytes.toBytes(3));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("characterVal"), Utils.serializeObject(new Character('a')));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("charVal"), Bytes.toBytes('a'));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("shortVal"), Utils.serializeObject(new Short((short) 2)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("shortPrimVal"), Bytes.toBytes((short) 2));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("byteVal"), Utils.serializeObject(new Byte((byte) 0xb)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("bytePrimVal"), Bytes.toBytes((byte) 0xb));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("floatVal"), Utils.serializeObject(new Float(1.01f)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("floatPrimVal"), Bytes.toBytes(1.01f));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("doubleVal"), Utils.serializeObject(new Double(2.22d)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("doublePrimVal"), Bytes.toBytes(2.22d));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("dateVal"), Utils.serializeObject(DATE1));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("ksEnum"), Utils.serializeObject(KitchenSinkEnum.ONE));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("bigDecimal"), Utils.serializeObject(new BigDecimal(2)));

        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("strArray"), Utils.serializeObject(new String[]{"a", "b"}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveIntArray"), Utils.serializeObject(new int[]{1, 2}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("integerArray"),
            Utils.serializeObject(new Integer[]{new Integer(3), new Integer(4)}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveLongArray"), Utils.serializeObject(new long[]{5L, 6L}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("longArray"), Utils.serializeObject(new Long[]{new Long(7L), new Long(8L)}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveShortArray"),
            Utils.serializeObject(new short[]{(short) 9, (short) 10}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("shortArray"),
            Utils.serializeObject(new Short[]{new Short((short) 11), new Short((short) 12)}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveCharArray"), Utils.serializeObject(new char[]{'a', 'b'}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("characterArray"),
            Utils.serializeObject(new Character[]{new Character('c'), new Character('d')}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveFloatArray"), Utils.serializeObject(new float[]{1.01f, 1.02f}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("floatArray"),
            Utils.serializeObject(new Float[]{new Float(1.03f), new Float(1.04f)}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveDoubleArray"), Utils.serializeObject(new double[]{2.01d, 2.02d}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("doubleArray"),
            Utils.serializeObject(new Double[]{new Double(2.03d), new Double(2.04d)}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("byteArray"),
            Utils.serializeObject(new Byte[]{new Byte((byte) 0xe), new Byte((byte) 0xf)}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveByteArray"), Utils.serializeObject(new byte[]{0xb, 0xc}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("primitiveBooleanArray"), Utils.serializeObject(new boolean[]{true, false}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("booleanArray"),
            Utils.serializeObject(new Boolean[]{Boolean.FALSE, Boolean.TRUE}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("dateArray"), Utils.serializeObject(new Date[]{DATE1, DATE2}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("ksEnumArray"),
            Utils.serializeObject(new KitchenSinkEnum[]{KitchenSinkEnum.TWO, KitchenSinkEnum.ONE}));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("bigDecimalArray"),
            Utils.serializeObject(new BigDecimal[]{new BigDecimal(3), new BigDecimal(4)}));

        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("strList"), Utils.serializeObject(Utils.newArrayList("p", "q")));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("integerList"), Utils.serializeObject(Utils.newArrayList(11, 12)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("longList"), Utils.serializeObject(Utils.newArrayList(13L, 14L)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("shortList"), Utils.serializeObject(Utils.newArrayList((short) 15, (short) 16)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("byteList"), Utils.serializeObject(Utils.newArrayList((byte) 0x8, (byte) 0x9)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("charList"), Utils.serializeObject(Utils.newArrayList('q', 'r')));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("doubleList"), Utils.serializeObject(Utils.newArrayList(22.44d, 23.55d)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("floatList"), Utils.serializeObject(Utils.newArrayList(23.44f, 24.55f)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("booleanList"), Utils.serializeObject(Utils.newArrayList(true, false)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("dateList"), Utils.serializeObject(Utils.newArrayList(DATE1, DATE2)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("ksEnumList"),
            Utils.serializeObject(Utils.newArrayList(KitchenSinkEnum.TWO, KitchenSinkEnum.ONE)));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("bigDecimalList"),
            Utils.serializeObject(Utils.newArrayList(new BigDecimal(7), new BigDecimal(6))));
        put.add(Bytes.toBytes("KitchenSink"), Bytes.toBytes("an extra property"), Bytes.toBytes("yar!"));

        return put;

    }

    public static final List<String> KITCHEN_SINK_FIELDS = getKitchenSinkFields();

    private static List<String> getKitchenSinkFields()
    {
        List<String> fields = Utils.newArrayList();
        for (Field f : KitchenSink.class.getDeclaredFields())
        {
            fields.add(f.getName());
        }
        return fields;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        KitchenSink that = (KitchenSink) o;

        if (boolPrimVal != that.boolPrimVal)
        {
            return false;
        }
        if (bytePrimVal != that.bytePrimVal)
        {
            return false;
        }
        if (charVal != that.charVal)
        {
            return false;
        }
        if (Double.compare(that.doublePrimVal, doublePrimVal) != 0)
        {
            return false;
        }
        if (Float.compare(that.floatPrimVal, floatPrimVal) != 0)
        {
            return false;
        }
        if (intVal != that.intVal)
        {
            return false;
        }
        if (longPrimVal != that.longPrimVal)
        {
            return false;
        }
        if (shortPrimVal != that.shortPrimVal)
        {
            return false;
        }
        if (bigDecimal != null ? !bigDecimal.equals(that.bigDecimal) : that.bigDecimal != null)
        {
            return false;
        }
        if (!Arrays.equals(bigDecimalArray, that.bigDecimalArray))
        {
            return false;
        }
        if (bigDecimalList != null ? !bigDecimalList.equals(that.bigDecimalList) : that.bigDecimalList != null)
        {
            return false;
        }
        if (boolVal != null ? !boolVal.equals(that.boolVal) : that.boolVal != null)
        {
            return false;
        }
        if (!Arrays.equals(booleanArray, that.booleanArray))
        {
            return false;
        }
        if (booleanList != null ? !booleanList.equals(that.booleanList) : that.booleanList != null)
        {
            return false;
        }
        if (!Arrays.equals(byteArray, that.byteArray))
        {
            return false;
        }
        if (byteList != null ? !byteList.equals(that.byteList) : that.byteList != null)
        {
            return false;
        }
        if (byteVal != null ? !byteVal.equals(that.byteVal) : that.byteVal != null)
        {
            return false;
        }
        if (charList != null ? !charList.equals(that.charList) : that.charList != null)
        {
            return false;
        }
        if (!Arrays.equals(characterArray, that.characterArray))
        {
            return false;
        }
        if (characterVal != null ? !characterVal.equals(that.characterVal) : that.characterVal != null)
        {
            return false;
        }
        if (!Arrays.equals(dateArray, that.dateArray))
        {
            return false;
        }
        if (dateList != null ? !dateList.equals(that.dateList) : that.dateList != null)
        {
            return false;
        }
        if (dateVal != null ? !dateVal.equals(that.dateVal) : that.dateVal != null)
        {
            return false;
        }
        if (!Arrays.equals(doubleArray, that.doubleArray))
        {
            return false;
        }
        if (doubleList != null ? !doubleList.equals(that.doubleList) : that.doubleList != null)
        {
            return false;
        }
        if (doubleVal != null ? !doubleVal.equals(that.doubleVal) : that.doubleVal != null)
        {
            return false;
        }
        if (!Arrays.equals(floatArray, that.floatArray))
        {
            return false;
        }
        if (floatList != null ? !floatList.equals(that.floatList) : that.floatList != null)
        {
            return false;
        }
        if (floatVal != null ? !floatVal.equals(that.floatVal) : that.floatVal != null)
        {
            return false;
        }
        if (!Arrays.equals(integerArray, that.integerArray))
        {
            return false;
        }
        if (integerList != null ? !integerList.equals(that.integerList) : that.integerList != null)
        {
            return false;
        }
        if (integerVal != null ? !integerVal.equals(that.integerVal) : that.integerVal != null)
        {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null)
        {
            return false;
        }
        if (ksEnum != that.ksEnum)
        {
            return false;
        }
        if (!Arrays.equals(ksEnumArray, that.ksEnumArray))
        {
            return false;
        }
        if (ksEnumList != null ? !ksEnumList.equals(that.ksEnumList) : that.ksEnumList != null)
        {
            return false;
        }
        if (!Arrays.equals(longArray, that.longArray))
        {
            return false;
        }
        if (longList != null ? !longList.equals(that.longList) : that.longList != null)
        {
            return false;
        }
        if (longVal != null ? !longVal.equals(that.longVal) : that.longVal != null)
        {
            return false;
        }
        if (!Arrays.equals(primitiveBooleanArray, that.primitiveBooleanArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveByteArray, that.primitiveByteArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveCharArray, that.primitiveCharArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveDoubleArray, that.primitiveDoubleArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveFloatArray, that.primitiveFloatArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveIntArray, that.primitiveIntArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveLongArray, that.primitiveLongArray))
        {
            return false;
        }
        if (!Arrays.equals(primitiveShortArray, that.primitiveShortArray))
        {
            return false;
        }
        if (!Arrays.equals(shortArray, that.shortArray))
        {
            return false;
        }
        if (shortList != null ? !shortList.equals(that.shortList) : that.shortList != null)
        {
            return false;
        }
        if (shortVal != null ? !shortVal.equals(that.shortVal) : that.shortVal != null)
        {
            return false;
        }
        if (!Arrays.equals(strArray, that.strArray))
        {
            return false;
        }
        if (strList != null ? !strList.equals(that.strList) : that.strList != null)
        {
            return false;
        }
        if (strVal != null ? !strVal.equals(that.strVal) : that.strVal != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = key != null ? key.hashCode() : 0;
        result = 31 * result + (strVal != null ? strVal.hashCode() : 0);
        result = 31 * result + (boolVal != null ? boolVal.hashCode() : 0);
        result = 31 * result + (boolPrimVal ? 1 : 0);
        result = 31 * result + (longVal != null ? longVal.hashCode() : 0);
        result = 31 * result + (int) (longPrimVal ^ (longPrimVal >>> 32));
        result = 31 * result + (integerVal != null ? integerVal.hashCode() : 0);
        result = 31 * result + intVal;
        result = 31 * result + (characterVal != null ? characterVal.hashCode() : 0);
        result = 31 * result + (int) charVal;
        result = 31 * result + (shortVal != null ? shortVal.hashCode() : 0);
        result = 31 * result + (int) shortPrimVal;
        result = 31 * result + (byteVal != null ? byteVal.hashCode() : 0);
        result = 31 * result + (int) bytePrimVal;
        result = 31 * result + (floatVal != null ? floatVal.hashCode() : 0);
        result = 31 * result + (floatPrimVal != +0.0f ? Float.floatToIntBits(floatPrimVal) : 0);
        result = 31 * result + (doubleVal != null ? doubleVal.hashCode() : 0);
        temp = doublePrimVal != +0.0d ? Double.doubleToLongBits(doublePrimVal) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (dateVal != null ? dateVal.hashCode() : 0);
        result = 31 * result + (ksEnum != null ? ksEnum.hashCode() : 0);
        result = 31 * result + (bigDecimal != null ? bigDecimal.hashCode() : 0);
        result = 31 * result + (strArray != null ? Arrays.hashCode(strArray) : 0);
        result = 31 * result + (primitiveIntArray != null ? Arrays.hashCode(primitiveIntArray) : 0);
        result = 31 * result + (integerArray != null ? Arrays.hashCode(integerArray) : 0);
        result = 31 * result + (primitiveLongArray != null ? Arrays.hashCode(primitiveLongArray) : 0);
        result = 31 * result + (longArray != null ? Arrays.hashCode(longArray) : 0);
        result = 31 * result + (primitiveShortArray != null ? Arrays.hashCode(primitiveShortArray) : 0);
        result = 31 * result + (shortArray != null ? Arrays.hashCode(shortArray) : 0);
        result = 31 * result + (primitiveCharArray != null ? Arrays.hashCode(primitiveCharArray) : 0);
        result = 31 * result + (characterArray != null ? Arrays.hashCode(characterArray) : 0);
        result = 31 * result + (primitiveFloatArray != null ? Arrays.hashCode(primitiveFloatArray) : 0);
        result = 31 * result + (floatArray != null ? Arrays.hashCode(floatArray) : 0);
        result = 31 * result + (primitiveDoubleArray != null ? Arrays.hashCode(primitiveDoubleArray) : 0);
        result = 31 * result + (doubleArray != null ? Arrays.hashCode(doubleArray) : 0);
        result = 31 * result + (primitiveByteArray != null ? Arrays.hashCode(primitiveByteArray) : 0);
        result = 31 * result + (byteArray != null ? Arrays.hashCode(byteArray) : 0);
        result = 31 * result + (primitiveBooleanArray != null ? Arrays.hashCode(primitiveBooleanArray) : 0);
        result = 31 * result + (booleanArray != null ? Arrays.hashCode(booleanArray) : 0);
        result = 31 * result + (dateArray != null ? Arrays.hashCode(dateArray) : 0);
        result = 31 * result + (ksEnumArray != null ? Arrays.hashCode(ksEnumArray) : 0);
        result = 31 * result + (bigDecimalArray != null ? Arrays.hashCode(bigDecimalArray) : 0);
        result = 31 * result + (strList != null ? strList.hashCode() : 0);
        result = 31 * result + (integerList != null ? integerList.hashCode() : 0);
        result = 31 * result + (longList != null ? longList.hashCode() : 0);
        result = 31 * result + (shortList != null ? shortList.hashCode() : 0);
        result = 31 * result + (charList != null ? charList.hashCode() : 0);
        result = 31 * result + (byteList != null ? byteList.hashCode() : 0);
        result = 31 * result + (doubleList != null ? doubleList.hashCode() : 0);
        result = 31 * result + (floatList != null ? floatList.hashCode() : 0);
        result = 31 * result + (booleanList != null ? booleanList.hashCode() : 0);
        result = 31 * result + (dateList != null ? dateList.hashCode() : 0);
        result = 31 * result + (ksEnumList != null ? ksEnumList.hashCode() : 0);
        result = 31 * result + (bigDecimalList != null ? bigDecimalList.hashCode() : 0);
        return result;
    }
}
