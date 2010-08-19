/**********************************************************************
Copyright (c) 2003 Mike Martin (TJDO) and others. All rights reserved.
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
package org.datanucleus.test.models;

import java.io.Serializable;
import java.util.Date;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * Qualification of a person.
 * @version $Revision: 1.1 $
 */
@PersistenceCapable(detachable = "true")
public class Qualification implements Serializable
{
    @Persistent
    private Person person;

    @Persistent
    @PrimaryKey
    private String name;

    @Persistent
    private Organisation organisation;

    @Persistent
    private Date date;

    /**
     * Default constructor required since this is a PersistenceCapable class.
     */
    public Qualification()
    {
    }

    public Qualification(String name)
    {
        this.name = name;
    }

    public void setPerson(Person mgr)
    {
        this.person = mgr;
    }

    public Person getPerson()
    {
        return this.person;
    }

    public void setDate(Date date)
    {
        this.date = date;
    }

    public Date getDate()
    {
        return this.date;
    }

    public void setOrganisation(Organisation org)
    {
        this.organisation = org;
    }

    public Organisation getOrganisation()
    {
        return this.organisation;
    }

    public String getName()
    {
        return this.name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String toString()
    {
        return "Qualification : " + name + " person=" + person;
    }
}
