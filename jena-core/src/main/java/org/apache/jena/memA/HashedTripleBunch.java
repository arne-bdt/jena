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

package org.apache.jena.memA;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;

public class HashedTripleBunch extends HashCommon<Triple, Triple> implements TripleBunch
    {
    private final Object indexingValue;
    public HashedTripleBunch( TripleBunch b )
        {
        super( nextSize( (int) (b.size() / loadFactor) ) );
        this.indexingValue = b.getIndexingValue();
        b.spliterator().forEachRemaining(t -> addUnchecked(t, t.hashCode()));
        changes = 0;
        }

    @Override
    public Object getIndexingValue()
        { return this.indexingValue; }

    @Override protected Triple mapValueToKey(Triple triple)
        { return triple; }

    @Override protected Triple[] newValueArray(int size )
        { return new Triple[size]; }


    @Override protected int findSlot(final Triple key, final int hashCodeOfKey )
        {
            int index = initialIndexFor( hashCodeOfKey );
            while (true)
            {
                final Triple current = values[index];
                if (current == null) return index;
                if (hashCodeOfKey == hashes[index] && key.equals( current )) return ~index;
                if (--index < 0) index += values.length;
            }
        }

    protected int findSlotBySameValueAs( final Triple key )
        {
        final int hash = key.hashCode();
        int index = initialIndexFor( hash );
        while (true)
            {
            final Object current = values[index];
            if (current == null) return index;
            if (hash == hashes[index] && key.matches( (Triple) current )) return ~index;
            if (--index < 0) index += values.length;
            }
        }

    @Override protected void grow()
        {
            final Triple [] oldContents = values;
            final int [] oldHashes = hashes;
            final Triple [] newValues = values = newValueArray(calcGrownCapacityAndSetThreshold());
            final int [] newHashes = hashes = new int[values.length];
            for (int i = 0; i < oldContents.length; i += 1)
            {
                if (null != oldContents[i])
                {
                    final int slot = findSlot( oldContents[i], oldHashes[i] );
                    newValues[slot] = oldContents[i];
                    newHashes[slot] = oldHashes[i];
                }
            }
        }

    @Override
    public boolean containsBySameValueAs( Triple t )
        { return findSlotBySameValueAs( t ) < 0; }

    /**
        Answer the number of items currently in this TripleBunch.
        @see TripleBunch#size()
    */
    @Override
    public int size()
        { return size; }

    /**
        Answer the current capacity of this HashedTripleBunch; for testing purposes
        only. [Note that the bunch is resized when it is more than half-occupied.]
    */
    public int currentCapacity()
        { return values.length; }

    @Override
    public boolean isHashed() { return true; }

    @Override
    public boolean add( Triple t, int hashCode )
        {
        var slot = findSlot( t, hashCode );
        if (slot < 0) return false;
        values[slot] = t;
        hashes[slot] = hashCode;
        changes++;
        if (++size > threshold) grow();
        return true;
        }

    @Override
    public void addUnchecked( Triple t, int hashCode )
        {
        final int slot = findSlot( t, hashCode );
        values[slot] = t;
        hashes[slot] = hashCode;
        changes++;
        if (++size > threshold) grow();
        }

    @Override
    public boolean add( Triple t ) { throw new UnsupportedOperationException(); }

    @Override
    public void addUnchecked( Triple t ) { throw new UnsupportedOperationException(); }


    @Override
    public boolean remove( Triple t, int hashCode )
        {
            if(super.tryRemoveKey( t, hashCode ))
                {
                changes++;
                return true;
                }
            return false;
        }

    @Override
    public void removeUnchecked( Triple t, int hashCode )
        {
            super.removeKey( t, hashCode );
            changes++;
        }

    @Override
    public boolean remove( Triple t ) { throw new UnsupportedOperationException(); }

    @Override
    public void removeUnchecked( Triple t ) { throw new UnsupportedOperationException(); }

    @Override
    public ExtendedIterator<Triple> iterator()
        { return iterator( NotifyEmpty.ignore ); }

    @Override
    public ExtendedIterator<Triple> iterator( final NotifyEmpty container )
        { return valueIterator( container ); }

    @Override public Spliterator<Triple> spliterator()
        { return super.valueSpliterator(); }
    }
