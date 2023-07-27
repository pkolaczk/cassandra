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

package org.apache.cassandra.index.sai.memory;

import java.io.IOException;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;

public class MemtableKeyRangeIterator extends KeyRangeIterator
{
    private final UnfilteredPartitionIterator partitionIterator;
    private UnfilteredRowIterator rowIterator;
    private final PrimaryKey.Factory pkFactory;


    private MemtableKeyRangeIterator(UnfilteredPartitionIterator partitionIterator,
                                     PrimaryKey.Factory pkFactory,
                                     PrimaryKey minKey,
                                     PrimaryKey maxKey,
                                     long count)
    {
        super(minKey, maxKey, count);
        this.partitionIterator = partitionIterator;
        this.rowIterator = null;
        this.pkFactory = pkFactory;
    }

    public static MemtableKeyRangeIterator create(Memtable memtable, AbstractBounds<PartitionPosition> keyRange)
    {
        TableMetadata metadata = memtable.metadata();
        PrimaryKey.Factory pkFactory = new PrimaryKey.Factory(metadata.comparator);
        Token minToken = keyRange.left.getToken();
        Token maxToken = keyRange.right.getToken();
        PrimaryKey minKey = pkFactory.createTokenOnly(minToken);
        PrimaryKey maxKey = pkFactory.createTokenOnly(maxToken);
        ColumnFilter columns = ColumnFilter.selectionBuilder()
                                           .addAll(metadata.partitionKeyColumns())
                                           .addAll(metadata.clusteringColumns())
                                           .build();
        DataRange dataRange = DataRange.forKeyRange(new Range<>(keyRange.left, keyRange.right));
        UnfilteredPartitionIterator partitionIterator = memtable.partitionIterator(columns, dataRange, null);
        long estimatedRowCount = memtable.operationCount(); // not exact but we don't know any better
        return new MemtableKeyRangeIterator(partitionIterator, pkFactory, minKey, maxKey, estimatedRowCount);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        // TODO: implement
        throw new UnsupportedOperationException("MemtableKeyRangeIterator skipTo not supported yet");
    }

    @Override
    public void close() throws IOException
    {
        partitionIterator.close();
        if (rowIterator != null)
            rowIterator.close();
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (hasNextRow(rowIterator) || partitionIterator.hasNext())
        {
            if (!hasNextRow(rowIterator))
            {
                rowIterator = partitionIterator.next();
                continue;
            }

            Unfiltered unfiltered = rowIterator.next();
            if (!unfiltered.isRow())
                continue;

            Row row = (Row) unfiltered;
            return pkFactory.create(rowIterator.partitionKey(), row.clustering());
        }
        return endOfData();
    }

    private static boolean hasNextRow(UnfilteredRowIterator rowIterator)
    {
        return rowIterator != null && rowIterator.hasNext();
    }
}
