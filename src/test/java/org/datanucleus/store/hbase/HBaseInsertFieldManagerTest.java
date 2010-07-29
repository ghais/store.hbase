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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

import javax.jdo.PersistenceManager;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
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
public class HBaseInsertFieldManagerTest extends BaseTest
{
    private static final Date DATE1 = new Date();

    private static final Date DATE2 = new Date();

    @Test
    public void testPrimativeFieldStorage() throws IOException
    {
        PersistenceManager pm = PMF.get().getPersistenceManager();
        JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
        final ClassLoaderResolver clr = new JDOClassLoaderResolver();
        final AbstractClassMetaData acmd = jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(HasSingleField.class, clr);
        ObjectProvider objectProviderMock = EasyMock.createMock(ObjectProvider.class);
        EasyMock.expect(objectProviderMock.getClassMetaData()).andReturn(acmd);
        EasyMock.expect(objectProviderMock.getExecutionContext()).andReturn(jpm.getObjectManager().getExecutionContext());
        EasyMock.replay(objectProviderMock);
        Put put = new Put(Bytes.toBytes("x"));
        Delete delete = new Delete(Bytes.toBytes("x"));
        HBaseInsertFieldManager fieldManager = new HBaseInsertFieldManager(objectProviderMock, put, delete)
        {
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
        };

        final byte[] family = Bytes.toBytes(Utils.getFamilyName(acmd, 0));
        final byte[] qualifier = Bytes.toBytes(Utils.getQualifierName(acmd, 0));
        fieldManager.storeBooleanField(0, true);
        assertTrue(put.has(family, qualifier, Bytes.toBytes(true)));
        fieldManager.storeByteField(0, (byte) 1);
        assertTrue(put.has(family, qualifier, Bytes.toBytes((byte) 1)));
        fieldManager.storeCharField(0, 'c');
        assertTrue(put.has(family, qualifier, Bytes.toBytes('c')));
        fieldManager.storeDoubleField(0, 1.1D);
        assertTrue(put.has(family, qualifier, Bytes.toBytes(1.1D)));
        fieldManager.storeFloatField(0, 1.1F);
        assertTrue(put.has(family, qualifier, Bytes.toBytes(1.1F)));
        fieldManager.storeIntField(0, 1);
        assertTrue(put.has(family, qualifier, Bytes.toBytes(1)));
        fieldManager.storeLongField(0, 1L);
        assertTrue(put.has(family, qualifier, Bytes.toBytes(1L)));
        fieldManager.storeShortField(0, (short) 1);
        assertTrue(put.has(family, qualifier, Bytes.toBytes((short) 1)));
        fieldManager.storeStringField(0, "s");
        assertTrue(put.has(family, qualifier, Bytes.toBytes("s")));
        fieldManager.storeObjectField(0, Boolean.TRUE);
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Boolean.TRUE)));
        fieldManager.storeObjectField(0, 4L);
        assertTrue(put.has(family, qualifier, Utils.serializeObject(4L)));
        fieldManager.storeObjectField(0, 3);
        assertTrue(put.has(family, qualifier, Utils.serializeObject(3)));
        fieldManager.storeObjectField(0, 'a');
        assertTrue(put.has(family, qualifier, Utils.serializeObject('a')));
        fieldManager.storeObjectField(0, (short) 2);
        assertTrue(put.has(family, qualifier, Utils.serializeObject((short) 2)));
        fieldManager.storeObjectField(0, (byte) 0xb);
        assertTrue(put.has(family, qualifier, Utils.serializeObject((byte) 0xb)));
        fieldManager.storeObjectField(0, 1.01f);
        assertTrue(put.has(family, qualifier, Utils.serializeObject(1.01f)));
        fieldManager.storeObjectField(0, 2.22d);
        assertTrue(put.has(family, qualifier, Utils.serializeObject(2.22d)));
        fieldManager.storeObjectField(0, MyEnum.ONE);
        assertTrue(put.has(family, qualifier, Utils.serializeObject(MyEnum.ONE)));
        fieldManager.storeObjectField(0, new BigDecimal(2.444d));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new BigDecimal(2.444d))));

        fieldManager.storeObjectField(0, new String[]{"a", "b"});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new String[]{"a", "b"})));
        fieldManager.storeObjectField(0, new int[]{1, 2});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new int[]{1, 2})));
        fieldManager.storeObjectField(0, new Integer[]{3, 4});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Integer[]{3, 4})));
        fieldManager.storeObjectField(0, new long[]{5L, 6L});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new long[]{5L, 6L})));
        fieldManager.storeObjectField(0, new Long[]{7L, 8L});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Long[]{7L, 8L})));
        fieldManager.storeObjectField(0, new short[]{(short) 9, (short) 10});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new short[]{(short) 9, (short) 10})));
        fieldManager.storeObjectField(0, new Short[]{(short) 11, (short) 12});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Short[]{(short) 11, (short) 12})));
        fieldManager.storeObjectField(0, new char[]{'a', 'b'});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new char[]{'a', 'b'})));
        fieldManager.storeObjectField(0, new Character[]{'c', 'd'});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Character[]{'c', 'd'})));
        fieldManager.storeObjectField(0, new float[]{1.01f, 1.02f});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new float[]{1.01f, 1.02f})));
        fieldManager.storeObjectField(0, new Float[]{1.03f, 1.04f});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Float[]{1.03f, 1.04f})));
        fieldManager.storeObjectField(0, new double[]{2.01d, 2.02d});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new double[]{2.01d, 2.02d})));
        fieldManager.storeObjectField(0, new Double[]{2.03d, 2.04d});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Double[]{2.03d, 2.04d})));
        fieldManager.storeObjectField(0, new byte[]{0xb, 0xc});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new byte[]{0xb, 0xc})));
        fieldManager.storeObjectField(0, new Byte[]{0xe, 0xf});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Byte[]{0xe, 0xf})));
        fieldManager.storeObjectField(0, new boolean[]{true, false});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new boolean[]{true, false})));
        fieldManager.storeObjectField(0, new Boolean[]{Boolean.FALSE, Boolean.TRUE});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Boolean[]{Boolean.FALSE, Boolean.TRUE})));
        fieldManager.storeObjectField(0, new Date[]{DATE1, DATE2});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new Date[]{DATE1, DATE2})));
        fieldManager.storeObjectField(0, new MyEnum[]{MyEnum.TWO, MyEnum.ONE});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new MyEnum[]{MyEnum.TWO, MyEnum.ONE})));
        fieldManager.storeObjectField(0, new BigDecimal[]{new BigDecimal(3.4444d), new BigDecimal(4.3333d)});
        assertTrue(put.has(family, qualifier, Utils.serializeObject(new BigDecimal[]{new BigDecimal(3.4444d), new BigDecimal(4.3333d)})));

        fieldManager.storeObjectField(0, Utils.newArrayList("p", "q"));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList("p", "q"))));
        fieldManager.storeObjectField(0, Utils.newArrayList(11, 12));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(11, 12))));
        fieldManager.storeObjectField(0, Utils.newArrayList(13L, 14L));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(13L, 14L))));
        fieldManager.storeObjectField(0, Utils.newArrayList((short) 15, (short) 16));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList((short) 15, (short) 16))));
        fieldManager.storeObjectField(0, Utils.newArrayList('q', 'r'));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList('q', 'r'))));
        fieldManager.storeObjectField(0, Utils.newArrayList((byte) 0x8, (byte) 0x9));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList((byte) 0x8, (byte) 0x9))));
        fieldManager.storeObjectField(0, Utils.newArrayList(22.44d, 23.55d));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(22.44d, 23.55d))));
        fieldManager.storeObjectField(0, Utils.newArrayList(23.44f, 24.55f));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(23.44f, 24.55f))));
        fieldManager.storeObjectField(0, Utils.newArrayList(true, false));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(true, false))));
        fieldManager.storeObjectField(0, Utils.newArrayList(DATE1, DATE2));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(DATE1, DATE2))));
        fieldManager.storeObjectField(0, Utils.newArrayList(MyEnum.TWO, MyEnum.ONE));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(MyEnum.TWO, MyEnum.ONE))));
        fieldManager.storeObjectField(0, Utils.newArrayList(new BigDecimal(7.6666d), new BigDecimal(6.7777d)));
        assertTrue(put.has(family, qualifier, Utils.serializeObject(Utils.newArrayList(new BigDecimal(7.6666d), new BigDecimal(6.7777d)))));

    }
}
