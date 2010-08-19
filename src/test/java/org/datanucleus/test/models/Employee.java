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

import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

/**
 * Employee in a company.
 * @version $Revision: 1.3 $
 */
@PersistenceCapable(detachable = "true")
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class Employee extends Person implements Serializable
{
    private String serialNo;

    private float salary;

    private String salaryCurrency;

    @Persistent(defaultFetchGroup = "false")
    private Integer yearsInCompany;

    @Persistent(embedded = "false", defaultFetchGroup = "true")
    private Manager manager;

    @Persistent
    private Account account;

    /** Used for the querying of static fields. */
    public static final String FIRSTNAME = "Bart";

    public Employee()
    {
    }

    public Employee(long id, String firstname, String lastname, String email, float sal, String serial)
    {
        super(id, firstname, lastname, email);
        this.salary = sal;
        this.serialNo = serial;
    }

    public Employee(long id, String firstname, String lastname, String email, float sal, String serial, Integer yearsInCompany)
    {
        super(id, firstname, lastname, email);
        this.salary = sal;
        this.serialNo = serial;
        this.yearsInCompany = yearsInCompany;
    }

    public Account getAccount()
    {
        return this.account;
    }

    public String getSerialNo()
    {
        return this.serialNo;
    }

    public float getSalary()
    {
        return this.salary;
    }

    public String getSalaryCurrency()
    {
        return this.salaryCurrency;
    }

    public synchronized Manager getManager()
    {
        return this.manager;
    }

    public Integer getYearsInCompany()
    {
        return this.yearsInCompany;
    }

    public void setManager(Manager mgr)
    {
        this.manager = mgr;
    }

    public void setAccount(Account acct)
    {
        this.account = acct;
    }

    public void setSerialNo(String sn)
    {
        this.serialNo = sn;
    }

    public void setSalary(float s)
    {
        this.salary = s;
    }

    public void setSalaryCurrency(String s)
    {
        this.salaryCurrency = s;
    }

    public void setYearsInCompany(Integer y)
    {
        this.yearsInCompany = y;
    }
}