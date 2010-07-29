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

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * Example 1: A has X B extends A has X X has a back-pointer to A Example 2: A B extends A has Y extends X Y has a
 * back-pointer to B Example 3: A has Y extends X B extends A X has a back-pointer to A
 * Based on Google App Engine Datanucleus plugin
 */
public class BidirectionalOneToOneSubclassesJDO
{

    public static class Example1
    {
        @PersistenceCapable(detachable = "true", table = "A1")
        @Inheritance(customStrategy = "complete-table")
        public static class A
        {
            @PrimaryKey
            @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
            private String id;

            private String aString;

            @Persistent(dependent = "true")
            private X child;

            public String getId()
            {
                return id;
            }

            public void setId(String id)
            {
                this.id = id;
            }

            public String getAString()
            {
                return aString;
            }

            public void setAString(String aString)
            {
                this.aString = aString;
            }

            public X getChild()
            {
                return child;
            }

            public void setChild(X child)
            {
                this.child = child;
            }
        }

        @PersistenceCapable(detachable = "true", table = "B1")
        public static class B extends A
        {
            private String bString;

            public String getBString()
            {
                return bString;
            }

            public void setBString(String bString)
            {
                this.bString = bString;
            }
        }

        @PersistenceCapable(detachable = "true", table = "X1")
        @Inheritance(customStrategy = "complete-table")
        public static class X
        {
            @PrimaryKey
            @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
            private String id;

            @Persistent(mappedBy = "child")
            private A parent;

            private String xString;

            public String getId()
            {
                return id;
            }

            public void setId(String id)
            {
                this.id = id;
            }

            public String getXString()
            {
                return xString;
            }

            public void setXString(String xString)
            {
                this.xString = xString;
            }
        }

        @PersistenceCapable(detachable = "true", table = "Y1")
        public static class Y extends X
        {
        }
    }

    public static class Example2
    {
        @PersistenceCapable(detachable = "true", table = "A2")
        @Inheritance(customStrategy = "complete-table")
        public static class A
        {
            @PrimaryKey
            @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
            private String id;

            private String aString;

            public String getId()
            {
                return id;
            }

            public void setId(String id)
            {
                this.id = id;
            }

            public String getAString()
            {
                return aString;
            }

            public void setAString(String aString)
            {
                this.aString = aString;
            }
        }

        @PersistenceCapable(detachable = "true", table = "B2")
        public static class B extends A
        {

            @Persistent(dependent = "true")
            private Y child;

            private String bString;

            public String getBString()
            {
                return bString;
            }

            public void setBString(String bString)
            {
                this.bString = bString;
            }

            public Y getChild()
            {
                return child;
            }

            public void setChild(Y child)
            {
                this.child = child;
            }
        }

        @PersistenceCapable(detachable = "true", table = "X2")
        @Inheritance(customStrategy = "complete-table")
        public static class X
        {
            @PrimaryKey
            @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
            private String id;

            private String xString;

            public String getId()
            {
                return id;
            }

            public void setId(String id)
            {
                this.id = id;
            }

            public String getXString()
            {
                return xString;
            }

            public void setXString(String xString)
            {
                this.xString = xString;
            }
        }

        @PersistenceCapable(detachable = "true", table = "Y2")
        public static class Y extends X
        {
            private String yString;

            @Persistent(mappedBy = "child")
            private B parent;

            public String getYString()
            {
                return yString;
            }

            public void setYString(String yString)
            {
                this.yString = yString;
            }

            public B getParent()
            {
                return parent;
            }

            public void setParent(B parent)
            {
                this.parent = parent;
            }
        }
    }

    public static class Example3
    {
        @PersistenceCapable(detachable = "true", table = "A3")
        @Inheritance(customStrategy = "complete-table")
        public static class A
        {
            @PrimaryKey
            @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
            private String id;

            private String aString;

            @Persistent(dependent = "true")
            private Y child;

            public String getId()
            {
                return id;
            }

            public void setId(String id)
            {
                this.id = id;
            }

            public String getAString()
            {
                return aString;
            }

            public void setAString(String aString)
            {
                this.aString = aString;
            }

            public Y getChild()
            {
                return child;
            }

            public void setChild(Y child)
            {
                this.child = child;
            }
        }

        @PersistenceCapable(detachable = "true", table = "B3")
        public static class B extends A
        {
            private String bString;

            public String getBString()
            {
                return bString;
            }

            public void setBString(String bString)
            {
                this.bString = bString;
            }
        }

        @PersistenceCapable(detachable = "true", table = "X3")
        @Inheritance(customStrategy = "complete-table")
        public static class X
        {
            @PrimaryKey
            @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
            private String id;

            @Persistent(mappedBy = "child")
            private A parent;

            private String xString;

            public String getId()
            {
                return id;
            }

            public void setId(String id)
            {
                this.id = id;
            }

            public String getXString()
            {
                return xString;
            }

            public void setXString(String xString)
            {
                this.xString = xString;
            }

            public A getParent()
            {
                return parent;
            }
        }

        @PersistenceCapable(detachable = "true", table = "Y3")
        public static class Y extends X
        {
            private String yString;

            public String getYString()
            {
                return yString;
            }

            public void setYString(String yString)
            {
                this.yString = yString;
            }
        }
    }
}