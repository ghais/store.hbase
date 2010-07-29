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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.PersistenceManagerFactory;

import org.datanucleus.OMFContext;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.hbase.MetaDataValidator.HBaseMetaDataException;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.HasIdentityStrategy;
import org.datanucleus.test.models.HasTwoPrimaryKeys;
import org.datanucleus.test.models.HasUniquenessConstraint;
import org.datanucleus.test.models.HasWrongTypeForSequence;
import org.datanucleus.test.models.Manager;
import org.datanucleus.test.models.Person;
import org.datanucleus.test.models.WrongPrimaryKeysType;
import org.junit.Test;

/**
 */
public class MetaDataValidatorTest extends BaseTest
{
    PersistenceManagerFactory pmf = PMF.get();

    @Test
    public void testIgnorableMapping_NoConfig()
    {
        setIgnorableMetaDataBehavior(null);
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        final String[] loggedMsg = {null};
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(Person.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, null)
        {
            @Override
            void warn(String msg)
            {
                loggedMsg[0] = msg;
            }
        };
        AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
        mdv.handleIgnorableMapping(ammd, "main msg", "warning only msg");
        assertTrue(loggedMsg[0].contains("main msg"));
        assertTrue(loggedMsg[0].contains("warning only msg"));
        assertTrue(loggedMsg[0].contains(MetaDataValidator.ADJUST_WARNING_MSG));
    }

    @Test
    public void testIgnorableMapping_NoneConfig()
    {
        setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.NONE.name());
        MetaDataManager mdm = ((JDOPersistenceManagerFactory) pmf).getOMFContext().getMetaDataManager();
        MetaDataValidator mdv = new MetaDataValidator(null, mdm, null)
        {
            @Override
            void warn(String msg)
            {
                fail("shouldn't have been called");
            }
        };
        mdv.handleIgnorableMapping(null, "main msg", "warning only msg");
        setIgnorableMetaDataBehavior(null);
    }

    @Test
    public void testIgnorableMapping_WarningConfig()
    {
        setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.WARN.name());
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        final String[] loggedMsg = {null};
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(Person.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, null)
        {
            @Override
            void warn(String msg)
            {
                loggedMsg[0] = msg;
            }
        };
        AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
        mdv.handleIgnorableMapping(ammd, "main msg", "warning only msg");
        assertTrue(loggedMsg[0].contains("main msg"));
        assertTrue(loggedMsg[0].contains("warning only msg"));
        assertTrue(loggedMsg[0].contains(MetaDataValidator.ADJUST_WARNING_MSG));
        setIgnorableMetaDataBehavior(null);
    }

    public void testIgnorableMapping_ErrorConfig()
    {
        setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.ERROR.name());
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(Person.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, null)
        {
            @Override
            void warn(String msg)
            {
                fail("shouldn't have been called");
            }
        };
        AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
        try
        {
            mdv.handleIgnorableMapping(ammd, "main msg", "warning only msg");
            fail("expected exception");
        }
        catch (MetaDataValidator.HBaseMetaDataException dmde)
        {
            assertTrue(dmde.getMessage().contains("main msg"));
            assertFalse(dmde.getMessage().contains("warning only msg"));
            assertFalse(dmde.getMessage().contains(MetaDataValidator.ADJUST_WARNING_MSG));
        }
        setIgnorableMetaDataBehavior(null);
    }

    private void setIgnorableMetaDataBehavior(String val)
    {
        ((JDOPersistenceManagerFactory) pmf).getOMFContext().getPersistenceConfiguration()
                .setProperty("datanucleus.hbase.ignorableMetaDataBehavior", val);
    }

    @Test(expected = HBaseMetaDataException.class)
    public void testHasTwoPrimaryKeys()
    {
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(HasTwoPrimaryKeys.class,
            omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, omfContext.getClassLoaderResolver(null));
        mdv.validate();
    }

    @Test(expected = HBaseMetaDataException.class)
    public void testHasObjectPrimaryKey()
    {
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(WrongPrimaryKeysType.class,
            omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, omfContext.getClassLoaderResolver(null));
        mdv.validate();
    }

    @Test(expected = HBaseMetaDataException.class)
    public void testSequenceIdentityStrategyWithStringKeys()
    {
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(HasWrongTypeForSequence.class,
            omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, omfContext.getClassLoaderResolver(null));
        mdv.validate();

    }

    @Test(expected = HBaseMetaDataException.class)
    public void testUniquenessConstraint()
    {
        setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.ERROR.name());
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(HasUniquenessConstraint.class,
            omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, omfContext.getClassLoaderResolver(null));
        mdv.validate();
        setIgnorableMetaDataBehavior(null);
    }

    @Test(expected = HBaseMetaDataException.class)
    public void testIdentityStrategy()
    {
        setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.ERROR.name());
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(HasIdentityStrategy.class,
            omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, omfContext.getClassLoaderResolver(null));
        mdv.validate();
        setIgnorableMetaDataBehavior(null);
    }

    @Test
    public void testPassMetaDataValidator()
    {
        setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.ERROR.name());
        OMFContext omfContext = ((JDOPersistenceManagerFactory) pmf).getOMFContext();
        MetaDataManager mdm = omfContext.getMetaDataManager();
        AbstractClassMetaData acmd = mdm.getMetaDataForClass(Manager.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
        MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, omfContext.getClassLoaderResolver(null));
        mdv.validate();
        setIgnorableMetaDataBehavior(null);
    }
}
