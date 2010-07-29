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
import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;

/**
 * Department in a company. Has a Manager, and a set of Projects being worked on.
 * @version $Revision: 1.1 $
 */
@PersistenceCapable(detachable = "true", identityType = IdentityType.APPLICATION)
public class Department implements Serializable
{
    @PrimaryKey
    private String name;

    private Manager manager;

    private Set<Project> projects = new HashSet<Project>();

    public Department(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return this.name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setManager(Manager mgr)
    {
        this.manager = mgr;
    }

    public Manager getManager()
    {
        return this.manager;
    }

    public Set<Project> getProjects()
    {
        return projects;
    }

    public void setProjects(Set<Project> projects)
    {
        this.projects = projects;
    }

    public void addProject(Project proj)
    {
        this.projects.add(proj);
    }

    public String toString()
    {
        return name;
    }
}