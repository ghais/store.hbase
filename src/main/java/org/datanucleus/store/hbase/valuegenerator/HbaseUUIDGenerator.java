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
package org.datanucleus.store.hbase.valuegenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.hbase.FatalNucleusUserException;
import org.datanucleus.store.hbase.HBaseConnectionPool;
import org.datanucleus.store.hbase.HBaseManagedConnection;
import org.datanucleus.store.hbase.Utils;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;

/**
 * @author Ghais Issa
 */
public class HBaseUUIDGenerator extends AbstractDatastoreGenerator
{
    /**
     * Constructor.
     *
     * @param name  Symbolic name for the generator
     * @param props Properties controlling the behaviour of the generator
     */
    public HBaseUUIDGenerator(String name, Properties props) {
        super(name, props);
    }


    @Override
    protected ValueGenerationBlock reserveBlock(long size) {
        List<String> ids = Utils.newArrayList();
        for(long i = 0; i < size; i++) {
            ids.add(UUID.randomUUID().toString());
        }
        return new ValueGenerationBlock(ids);
    }
}
