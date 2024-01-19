/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk;

import org.apache.cassandra.index.sai.utils.PrimaryKey;

public abstract class SSTableKeyTracker
{
    public PrimaryKey minKey;
    public PrimaryKey maxKey;
    public PrimaryKey minStaticKey;
    public PrimaryKey maxStaticKey;

    public long maxSSTableRowId = -1;

    protected int count;
    
    protected void updateMinMaxKeys(PrimaryKey key)
    {
        // Data is written in primary key order. If a schema contains clustering keys, it may also contain static
        // columns. We track the min and max static keys separately here so that we can pass them to the segment
        // metadata for indexes on static columns.
        if (key.kind() == PrimaryKey.Kind.STATIC)
        {
            if (minStaticKey == null)
                minStaticKey = key;
            maxStaticKey = key;
        }
        else
        {
            if (minKey == null)
                minKey = key;
            maxKey = key;
        }
    }

    public int getCount()
    {
        return count;
    }
}
