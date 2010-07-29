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

package org.datanucleus.test.models;

import java.io.Serializable;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Version;
import javax.jdo.annotations.VersionStrategy;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(detachable = "true")
@Version(strategy = VersionStrategy.VERSION_NUMBER)
public class Flight implements Serializable
{
    private String id;

    @Persistent
    private String origin;

    @Persistent
    private String dest;

    @Persistent
    private String name;

    @Persistent
    int you;

    @Persistent
    int me;

    @Persistent
    @Column(name = "flight_number")
    int flightNumber;

    public Flight(String origin, String dest, String name, int you, int me)
    {
        this.origin = origin;
        this.dest = dest;
        this.name = name;
        this.you = you;
        this.me = me;
    }

    /**
     * 
     */
    public Flight()
    {
    }

    public String getOrigin()
    {
        return origin;
    }

    public void setOrigin(String origin)
    {
        this.origin = origin;
    }

    public String getDest()
    {
        return dest;
    }

    public void setDest(String dest)
    {
        this.dest = dest;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getYou()
    {
        return you;
    }

    public void setYou(int you)
    {
        this.you = you;
    }

    public int getMe()
    {
        return me;
    }

    public void setMe(int me)
    {
        this.me = me;
    }

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
    public String getId()
    {
        return id;
    }

    /**
     * You really shouldn't call this unless you're looking for trouble. Useful for tests that verify the trouble.
     */
    public void setId(String id)
    {
        this.id = id;
    }

    public int getFlightNumber()
    {
        return flightNumber;
    }

    public void setFlightNumber(int flightNumber)
    {
        this.flightNumber = flightNumber;
    }

    @Override
    public String toString()
    {
        return "\n\nid: " + id + "\norigin: " + origin + "\ndest: " + dest + "\nname: " + name + "\nyou: " + you + "\nme: " + me;
    }

    /**
     * We get weird test failures if we give Flight an equals() method due to attempts to read fields of deleted
     * objects. TODO(maxr) Straighten this out.
     */
    public boolean customEquals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        Flight flight = (Flight) o;

        if (flightNumber != flight.flightNumber)
        {
            return false;
        }
        if (me != flight.me)
        {
            return false;
        }
        if (you != flight.you)
        {
            return false;
        }
        if (dest != null ? !dest.equals(flight.dest) : flight.dest != null)
        {
            return false;
        }
        if (id != null ? !id.equals(flight.id) : flight.id != null)
        {
            return false;
        }
        if (name != null ? !name.equals(flight.name) : flight.name != null)
        {
            return false;
        }
        if (origin != null ? !origin.equals(flight.origin) : flight.origin != null)
        {
            return false;
        }

        return true;
    }

    public static void addData(Flight f, String name, String origin, String dest, int you, int me, int flightNumber)
    {
        f.setName(name);
        f.setOrigin(origin);
        f.setDest(dest);
        f.setYou(you);
        f.setMe(me);
        f.setFlightNumber(flightNumber);
    }

    public static Put newFlightPut(String id, String name, String origin, String dest, int you, int me, int flightNumber)
    {
        Put put = new Put(Bytes.toBytes(id));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("id"), Bytes.toBytes(id));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("origin"), Bytes.toBytes(origin));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("dest"), Bytes.toBytes(dest));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("you"), Bytes.toBytes(you));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("me"), Bytes.toBytes(me));
        put.add(Bytes.toBytes("Flight"), Bytes.toBytes("flightNumber"), Bytes.toBytes(flightNumber));
        return put;
    }
}
