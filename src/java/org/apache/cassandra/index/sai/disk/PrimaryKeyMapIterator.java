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

import java.io.IOException;

import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Iterates all keys in the PrimaryKeyMap
 */
public final class PrimaryKeyMapIterator extends KeyRangeIterator
{
    private final PrimaryKeyMap keys;
    private long currentRowId = 0;

    private PrimaryKeyMapIterator(PrimaryKeyMap keys, PrimaryKey min, PrimaryKey max)
    {
        super(min, max, keys.count());
        this.keys = keys;
    }

    public static PrimaryKeyMapIterator create(PrimaryKeyMap.Factory keyMapFactory) throws IOException
    {
        PrimaryKeyMap keys = keyMapFactory.newPerSSTablePrimaryKeyMap();
        long count = keys.count();
        PrimaryKey minKey = count > 0 ? keys.primaryKeyFromRowId(0) : null;
        PrimaryKey maxKey = count > 0 ? keys.primaryKeyFromRowId(count - 1) : null;
        return new PrimaryKeyMapIterator(keys, minKey, maxKey);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        this.currentRowId = keys.rowIdFromPrimaryKey(nextKey);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        return currentRowId < keys.count()
               ? keys.primaryKeyFromRowId(currentRowId++)
               : endOfData();
    }

    @Override
    public void close() throws IOException
    {
        keys.close();
    }

}
