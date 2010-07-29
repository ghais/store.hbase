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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;

import javax.jdo.PersistenceManager;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.JDOClassLoaderResolver;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.HasSingleField;
import org.datanucleus.test.models.MyEnum;
import org.easymock.EasyMock;
import org.junit.Test;

/**
 */
public class HBaseFetchFieldManagerTest extends BaseTest
{

    private ClassLoaderResolver clr = null;

    private AbstractClassMetaData acmd = null;

    private static final Date DATE1 = new Date();

    private static final Date DATE2 = new Date();

    @Test
    public void testFetch() throws IOException
    {
        PersistenceManager pm = PMF.get().getPersistenceManager();
        JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
        clr = new JDOClassLoaderResolver();
        acmd = jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(HasSingleField.class, clr);
        ObjectProvider objectProviderMock = EasyMock.createMock(ObjectProvider.class);
        EasyMock.expect(objectProviderMock.getClassMetaData()).andReturn(acmd);
        EasyMock.expect(objectProviderMock.getExecutionContext()).andReturn(jpm.getObjectManager().getExecutionContext());
        EasyMock.replay(objectProviderMock);

        final byte[] family = Bytes.toBytes(Utils.getFamilyName(acmd, 0));
        final byte[] qualifier = Bytes.toBytes(Utils.getQualifierName(acmd, 0));
        final byte[] row = Bytes.toBytes("row123");
        Result result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes("strVal"))});
        assertEquals("strVal", new MockHBaseFetchFieldManager(objectProviderMock, result).fetchStringField(0));
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(true))});
        assertEquals(true, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchBooleanField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Boolean(true)))});
        assertEquals(new Boolean(true), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchBooleanField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(4L))});
        assertEquals(4L, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchLongField(0));
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Long(4)))});
        assertTrue(new Long(4) == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchLongField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(3))});
        assertEquals(3, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchIntField(0));
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Integer(3)))});
        assertTrue(new Integer(3) == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchIntField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes('c'))});
        assertEquals('c', new MockHBaseFetchFieldManager(objectProviderMock, result).fetchCharField(0));
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Character('c')))});
        assertTrue(new Character('c') == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchCharField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes((short) 2))});
        assertEquals((short) 2, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchShortField(0));
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Short((short) 2)))});
        assertTrue(new Short((short) 2) == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchShortField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes((byte) 0xb))});
        assertEquals((byte) 0xb, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchByteField(0));
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Byte((byte) 0xb)))});
        assertTrue(new Byte((byte) 0xb) == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchByteField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(1.01f))});
        assertEquals(1.01f, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchFloatField(0), 0);
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Float(1.01f)))});
        assertTrue(new Float(1.01f) == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchFloatField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(2.22d))});
        assertEquals(2.22d, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchDoubleField(0), 0);
        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Bytes.toBytes(new Double(2.22d)))});
        assertTrue(new Double(2.22d) == new MockHBaseFetchFieldManager(objectProviderMock, result).fetchDoubleField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(DATE1))});
        assertEquals(DATE1, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(MyEnum.ONE))});
        assertEquals(MyEnum.ONE, new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new BigDecimal(2.444d)))});
        assertEquals(new BigDecimal(2.444d), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new String[]{"a", "b"}))});
        assertTrue(Arrays.equals(new String[]{"a", "b"},
            (String[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new int[]{1, 2}))});
        assertTrue(Arrays.equals(new int[]{1, 2}, (int[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Integer[]{3, 4}))});
        assertTrue(Arrays.equals(new Integer[]{3, 4},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new long[]{5L, 6L}))});
        assertTrue(Arrays.equals(new long[]{5L, 6L},
            (long[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Long[]{7L, 8L}))});
        assertTrue(Arrays.equals(new Long[]{7L, 8L},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new short[]{(short) 9, (short) 10}))});
        assertTrue(Arrays.equals(new short[]{(short) 9, (short) 10},
            (short[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(
                new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Short[]{(short) 11, (short) 12}))});
        assertTrue(Arrays.equals(new Short[]{(short) 11, (short) 12},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new char[]{'a', 'b'}))});
        assertTrue(Arrays.equals(new char[]{'a', 'b'},
            (char[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Character[]{'c', 'd'}))});
        assertTrue(Arrays.equals(new Character[]{'c', 'd'},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new float[]{1.01f, 1.02f}))});
        assertTrue(Arrays.equals(new float[]{1.01f, 1.02f},
            (float[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Float[]{1.03f, 1.04f}))});
        assertTrue(Arrays.equals(new Float[]{1.03f, 1.04f},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new double[]{2.01d, 2.02d}))});
        assertTrue(Arrays.equals(new double[]{2.01d, 2.02d},
            (double[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Double[]{2.03d, 2.04d}))});
        assertTrue(Arrays.equals(new Double[]{2.03d, 2.04d},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new byte[]{0xb, 0xc}))});
        assertTrue(Arrays.equals(new byte[]{0xb, 0xc},
            (byte[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Byte[]{0xe, 0xf}))});
        assertTrue(Arrays.equals(new Byte[]{0xe, 0xf},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new boolean[]{true, false}))});
        assertTrue(Arrays.equals(new boolean[]{true, false},
            (boolean[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Boolean[]{Boolean.FALSE,
                Boolean.TRUE}))});
        assertTrue(Arrays.equals(new Boolean[]{Boolean.FALSE, Boolean.TRUE}, (Object[]) new MockHBaseFetchFieldManager(objectProviderMock,
                result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new Date[]{DATE1, DATE2}))});
        assertTrue(Arrays.equals(new Date[]{DATE1, DATE2},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(
                new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new MyEnum[]{MyEnum.TWO, MyEnum.ONE}))});
        assertTrue(Arrays.equals(new MyEnum[]{MyEnum.TWO, MyEnum.ONE},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(new BigDecimal[]{
                new BigDecimal(3.4444d), new BigDecimal(4.3333d)}))});
        assertTrue(Arrays.equals(new BigDecimal[]{new BigDecimal(3.4444d), new BigDecimal(4.3333d)},
            (Object[]) new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0)));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList("p", "q")))});
        assertEquals(Utils.newArrayList("p", "q"), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(11, 12)))});
        assertEquals(Utils.newArrayList(11, 12), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(13L, 14L)))});
        assertEquals(Utils.newArrayList(13L, 14L), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList((short) 15,
            (short) 16)))});
        assertEquals(Utils.newArrayList((short) 15, (short) 16),
            new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList('q', 'r')))});
        assertEquals(Utils.newArrayList('q', 'r'), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList((byte) 0x8,
            (byte) 0x9)))});
        assertEquals(Utils.newArrayList((byte) 0x8, (byte) 0x9),
            new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(22.44d, 23.55d)))});
        assertEquals(Utils.newArrayList(22.44d, 23.55d), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(23.44f, 24.55f)))});
        assertEquals(Utils.newArrayList(23.44f, 24.55f), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(true, false)))});
        assertEquals(Utils.newArrayList(true, false), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(DATE1, DATE2)))});
        assertEquals(Utils.newArrayList(DATE1, DATE2), new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(MyEnum.TWO,
            MyEnum.ONE)))});
        assertEquals(Utils.newArrayList(MyEnum.TWO, MyEnum.ONE),
            new MockHBaseFetchFieldManager(objectProviderMock, result).fetchObjectField(0));

        result = new Result(new KeyValue[]{new KeyValue(row, family, qualifier, Utils.serializeObject(Utils.newArrayList(new BigDecimal(
                7.6666d), new BigDecimal(6.7777d))))});
        assertEquals(Utils.newArrayList(new BigDecimal(7.6666d), new BigDecimal(6.7777d)), new MockHBaseFetchFieldManager(
                objectProviderMock, result).fetchObjectField(0));
    }

    private class MockHBaseFetchFieldManager extends HBaseFetchFieldManager
    {

        /**
         * @param objectProvider
         * @param result
         */
        public MockHBaseFetchFieldManager(ObjectProvider objectProvider, Result result)
        {
            super(objectProvider, result);
        }

        @Override
        AbstractClassMetaData getClassMetaData()
        {
            return acmd;
        }

        @Override
        ClassLoaderResolver getClassLoaderResolver()
        {
            return clr;
        }
    }
}
