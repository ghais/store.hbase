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

import org.datanucleus.exceptions.NucleusUserException;

/**
 * Shortcut for new NucleusUserException().setFatal();. Based on the work of datanuclues-appengine
 */
public class FatalNucleusUserException extends NucleusUserException
{

    public FatalNucleusUserException(String msg)
    {
        super(msg);
        setFatal();
    }

    public FatalNucleusUserException(String msg, Exception e)
    {
        super(msg, e);
        setFatal();
    }
}
