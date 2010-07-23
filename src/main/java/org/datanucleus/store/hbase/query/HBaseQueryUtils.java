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
package org.datanucleus.store.hbase.query;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues2;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.Type;
import org.datanucleus.store.hbase.HBaseFetchFieldManager;
import org.datanucleus.store.hbase.HBaseManagedConnection;
import org.datanucleus.store.hbase.HBaseUtils;

class HBaseQueryUtils
{
    /**
     * Convenience method to get all objects of the candidate type (and optional subclasses) from the 
     * specified XML connection.
     * @param om ObjectManager
     * @param mconn Managed Connection
     * @param candidateClass Candidate
     * @param subclasses Include subclasses?
     * @param ignoreCache Whether to ignore the cache
     * @return List of objects of the candidate type (or subclass)
     */
    static List getObjectsOfCandidateType(final ExecutionContext om, final HBaseManagedConnection mconn,
            Class candidateClass, boolean subclasses, boolean ignoreCache)
    {
        List results = new ArrayList();
        try
        {
            final ClassLoaderResolver clr = om.getClassLoaderResolver();
            final AbstractClassMetaData acmd = om.getMetaDataManager().getMetaDataForClass(candidateClass,clr);

            Iterator<Result> it = (Iterator<Result>) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));

                    Scan scan = new Scan();
                    int[] fieldNumbers =  acmd.getAllMemberPositions();
                    for(int i=0; i<fieldNumbers.length; i++)
                    {
                        byte[] familyNames = HBaseUtils.getFamilyName(acmd,fieldNumbers[i]).getBytes();
                        byte[] columnNames = HBaseUtils.getQualifierName(acmd, fieldNumbers[i]).getBytes();
                        scan.addColumn(familyNames,columnNames);
                    }
                    ResultScanner scanner = table.getScanner(scan);
                    Iterator<Result> it = scanner.iterator();
                    return it;
                }
            });
            
            while(it.hasNext())
            {
                final Result result = it.next();
                results.add(om.findObjectUsingAID(new Type(clr.classForName(acmd.getFullClassName())), new FieldValues2()
                {
                    // StateManager calls the fetchFields method
                    public void fetchFields(ObjectProvider sm)
                    {
                        sm.replaceFields(acmd.getPKMemberPositions(), new HBaseFetchFieldManager(acmd, result));
                        sm.replaceFields(acmd.getBasicMemberPositions(clr, om.getMetaDataManager()), new HBaseFetchFieldManager(acmd, result));
                    }

                    public void fetchNonLoadedFields(ObjectProvider sm)
                    {
                        sm.replaceNonLoadedFields(acmd.getAllMemberPositions(), new HBaseFetchFieldManager(acmd, result));
                    }

                    public FetchPlan getFetchPlanForLoading()
                    {
                        return null;
                    }
                }, ignoreCache, true));

            }
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
        return results;
    }
}