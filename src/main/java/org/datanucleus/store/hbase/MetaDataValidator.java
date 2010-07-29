/**********************************************************************
Copyright (c) 2010 Ghais Issa and others. All rights reserved.
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

import java.util.Map;
import java.util.Set;

import javax.jdo.identity.SingleFieldIdentity;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.util.NucleusLogger;

/**
 * Hbase specific rules for Meta Data. Adapted from the Google App Engine datanucleus plugin
 * @author Ghais Issa
 */
public class MetaDataValidator
{

    /**
     * Defines the various actions we can take when we encounter ignorable meta-data.
     */
    enum IgnorableMetaDataBehavior {
        NONE, // Do nothing at all.
        WARN, // Log a warning.
        ERROR;// Throw an exception.

        private static IgnorableMetaDataBehavior valueOf(String val, IgnorableMetaDataBehavior returnIfNull)
        {
            if (val == null)
            {
                return returnIfNull;
            }
            return valueOf(val);
        }
    }

    /**
     * Config property that determines the action we take when we encounter ignorable meta-data.
     */
    private static final String IGNORABLE_META_DATA_BEHAVIOR_PROPERTY = "datanucleus.hbase.ignorableMetaDataBehavior";

    /**
     * This message is appended to every ignorable meta-data warning so users know they can configure it.
     */
    static final String ADJUST_WARNING_MSG = String
            .format(
                "You can modify this warning by setting the %s property in your config.  " + "A value of %s will silence the warning.  " + "A value of %s will turn the warning into an exception.",
                IGNORABLE_META_DATA_BEHAVIOR_PROPERTY, IgnorableMetaDataBehavior.NONE, IgnorableMetaDataBehavior.ERROR);

    private final AbstractClassMetaData acmd;

    private final MetaDataManager metaDataManager;

    private final ClassLoaderResolver clr;

    public MetaDataValidator(AbstractClassMetaData acmd, MetaDataManager metaDataManager, ClassLoaderResolver clr)
    {
        this.acmd = acmd;
        this.metaDataManager = metaDataManager;
        this.clr = clr;
    }

    public void validate()
    {
        NucleusLogger.METADATA.info("Performing hbase-specific metadata validation for " + acmd.getFullClassName());
        AbstractMemberMetaData pkMemberMetaData = validatePrimaryKey();
        validateFields(pkMemberMetaData);
        validateClass();
        NucleusLogger.METADATA.info("Finished performing hbase-specific metadata validation for " + acmd.getFullClassName());
    }

    private void validateClass()
    {
        // Look for uniqueness constraints. Not supported but not necessarily an error
        if (acmd.getUniqueMetaData() != null && acmd.getUniqueMetaData().length > 0)
        {
            handleIgnorableMapping("The datastore does not support uniqueness constraints.", "The constraint definition will be ignored.");
        }

        if (!SingleFieldIdentity.class.isAssignableFrom(clr.classForName(acmd.getObjectidClass())))
        {
            handleIgnorableMapping("The datastore does not custom objectIdClasses.", "The custom objectIdClass will be ignored.");
        }
    }

    private void validateFields(AbstractMemberMetaData pkMemberMetaData)
    {
        Set<String> foundOneOrZeroExtensions = Utils.newHashSet();
        Map<Class<?>, String> nonRepeatableRelationTypes = Utils.newHashMap();
        Class<?> pkClass = pkMemberMetaData.getType();

        // the constraints that we check across all fields apply to the entire
        // persistent class hierarchy so we're going to validate every field
        // at every level of the hierarchy. As an example, this lets us detect
        // multiple one-to-many relationships at different levels of the class
        // hierarchy
        AbstractClassMetaData curCmd = acmd;
        do
        {
            for (AbstractMemberMetaData ammd : curCmd.getManagedMembers())
            {
                validateField(pkMemberMetaData, pkClass, foundOneOrZeroExtensions, nonRepeatableRelationTypes, ammd);
            }
            curCmd = curCmd.getSuperAbstractClassMetaData();
        }
        while (curCmd != null);
    }

    private void validateField(AbstractMemberMetaData pkMemberMetaData, Class<?> pkClass, Set<String> foundOneOrZeroExtensions,
            Map<Class<?>, String> nonRepeatableRelationTypes, AbstractMemberMetaData ammd)
    {

        checkForIllegalChildField(ammd);

        if (ammd.getValueGeneratorName() != null)
        {
            SequenceMetaData sequenceMetaData = metaDataManager.getMetaDataForSequence(clr, ammd.getValueGeneratorName());
            if (sequenceMetaData != null && sequenceMetaData.getInitialValue() != 1)
            {
                throw new HBaseMetaDataException(acmd, ammd, "HBase doesnt' currently support sequencing");
            }
        }
    }

    private IgnorableMetaDataBehavior getIgnorableMetaDataBehavior()
    {
        return IgnorableMetaDataBehavior.valueOf(
            metaDataManager.getOMFContext().getPersistenceConfiguration().getStringProperty(IGNORABLE_META_DATA_BEHAVIOR_PROPERTY),
            IgnorableMetaDataBehavior.WARN);
    }

    void handleIgnorableMapping(String msg, String warningOnlyMsg)
    {
        handleIgnorableMapping(null, msg, warningOnlyMsg);
    }

    void handleIgnorableMapping(AbstractMemberMetaData ammd, String msg, String warningOnlyMsg)
    {
        switch (getIgnorableMetaDataBehavior())
        {
            case WARN :
                if (ammd == null)
                {
                    warn(String.format("Meta-data warning for %s: %s  %s  %s", acmd.getFullClassName(), msg, warningOnlyMsg,
                        ADJUST_WARNING_MSG));
                }
                else
                {
                    warn(String.format("Meta-data warning for %s.%s: %s  %s  %s", acmd.getFullClassName(), ammd.getName(), msg,
                        warningOnlyMsg, ADJUST_WARNING_MSG));
                }
                break;
            case ERROR :
                if (ammd == null)
                {
                    throw new HBaseMetaDataException(acmd, msg);
                }
                throw new HBaseMetaDataException(acmd, ammd, msg);
                // We swallow both null and NONE
        }
    }

    // broken out for testing
    void warn(String msg)
    {
        NucleusLogger.METADATA.warn(msg);
    }

    private void checkForIllegalChildField(AbstractMemberMetaData ammd)
    {
        // Figure out if this field is the owning side of a one to one or a one to
        // many. If it is, look at the mapping of the child class and make sure their
        // pk isn't Long or unencoded String.
        int relationType = ammd.getRelationType(clr);
        if (relationType == Relation.NONE || ammd.isEmbedded())
        {
            return;
        }
        AbstractClassMetaData childAcmd = null;
        if (relationType == Relation.ONE_TO_MANY_BI || relationType == Relation.ONE_TO_MANY_UNI || relationType == Relation.MANY_TO_MANY_BI)
        {
            if (ammd.getCollection() != null)
            {
                childAcmd = ammd.getCollection().getElementClassMetaData(clr, metaDataManager);
            }
            else if (ammd.getArray() != null)
            {
                childAcmd = ammd.getArray().getElementClassMetaData(clr, metaDataManager);
            }
            else if (ammd.getMap() != null)
            {
                childAcmd = ammd.getMap().getValueClassMetaData(clr, metaDataManager);
            }
            else
            {
                // don't know how to verify
                NucleusLogger.METADATA.warn("Unable to validate relation " + ammd.getFullFieldName());
            }
            if (ammd.getOrderMetaData() != null)
            {
                verifyOneToManyOrderBy(ammd, childAcmd);
            }
        }
        else if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI)
        {
            childAcmd = metaDataManager.getMetaDataForClass(ammd.getType(), clr);
        }
        if (childAcmd == null)
        {
            return;
        }

        // Get the type of the primary key of the child
        int[] pkPositions = childAcmd.getPKMemberPositions();
        if (pkPositions == null)
        {
            // don't know how to verify
            NucleusLogger.METADATA.warn("Unable to validate relation " + ammd.getFullFieldName());
            return;
        }
        int pkPos = pkPositions[0];
        AbstractMemberMetaData pkMemberMetaData = childAcmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);
        Class<?> pkType = pkMemberMetaData.getType();
        if (!(pkType.equals(Long.class) || pkType.equals(Long.TYPE) || pkType.equals(String.class)))
        {
            throw new HBaseMetaDataException(childAcmd, pkMemberMetaData,
                    "Cannot have a " + pkType.getName() + " primary key and be a child object " + "(owning field is " + ammd
                            .getFullFieldName() + ").");
        }
    }

    private void verifyOneToManyOrderBy(AbstractMemberMetaData ammd, AbstractClassMetaData childAcmd)
    {
        OrderMetaData omd = ammd.getOrderMetaData();
        OrderMetaData.FieldOrder[] fieldOrders = omd.getFieldOrders();
        if (fieldOrders == null)
        {
            return;
        }
        for (OrderMetaData.FieldOrder fieldOrder : omd.getFieldOrders())
        {
            String propertyName = fieldOrder.getFieldName();
            AbstractMemberMetaData orderField = childAcmd.getMetaDataForMember(propertyName);
            if (orderField == null)
            {
                // shouldn't happen since DataNuc does the same check in omd.getFieldOrders()
                throw new HBaseMetaDataException(acmd, ammd,
                        "Order property " + propertyName + " could not be founcd on " + childAcmd.getFullClassName());
            }
        }
    }

    private AbstractMemberMetaData validatePrimaryKey()
    {
        int[] pkPositions = acmd.getPKMemberPositions();
        if (pkPositions == null)
        {
            throw new HBaseMetaDataException(acmd, "No primary key defined.");
        }
        if (pkPositions.length != 1)
        {
            throw new HBaseMetaDataException(acmd, "More than one primary key field.");
        }
        int pkPos = pkPositions[0];
        AbstractMemberMetaData pkMemberMetaData = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);

        Class<?> pkType = pkMemberMetaData.getType();
        if (pkType.equals(String.class))
        {

            // encoded string pk
            if (hasIdentityStrategy(IdentityStrategy.SEQUENCE, pkMemberMetaData))
            {
                throw new HBaseMetaDataException(acmd, pkMemberMetaData,
                        "IdentityStrategy SEQUENCE is not supported on encoded String primary keys.");
            }

        }
        else if (pkType.equals(Long.class))
        {
            // do nothing
        }
        else
        {
            throw new HBaseMetaDataException(acmd, pkMemberMetaData, "Unsupported primary key type: " + pkType.getName());
        }
        return pkMemberMetaData;
    }

    private static boolean hasIdentityStrategy(IdentityStrategy strat, AbstractMemberMetaData ammd)
    {
        return ammd.getValueStrategy() != null && ammd.getValueStrategy().equals(strat);
    }

    static final class HBaseMetaDataException extends NucleusUserException
    {

        private static final long serialVersionUID = -2985743075925044859L;

        private static final String MSG_FORMAT_CLASS_ONLY = "Error in meta-data for %s: %s";

        private static final String MSG_FORMAT = "Error in meta-data for %s.%s: %s";

        private HBaseMetaDataException(AbstractClassMetaData acmd, String msg)
        {
            super(String.format(MSG_FORMAT_CLASS_ONLY, acmd.getFullClassName(), msg));
        }

        private HBaseMetaDataException(AbstractClassMetaData acmd, AbstractMemberMetaData ammd, String msg)
        {
            super(String.format(MSG_FORMAT, acmd.getFullClassName(), ammd.getName(), msg));
        }

        @Override
        public boolean isFatal()
        {
            // Always fatal
            return true;
        }
    }
}
