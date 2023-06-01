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

package org.apache.jena.memTermEquality;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;
import java.util.function.Predicate;

public class HashedTripleBunch extends HashCommon<Triple> implements TripleBunch
    {
    public HashedTripleBunch( TripleBunch b )
        {
        super( nextSize( (int) (b.size() / loadFactor) ) );
        b.spliterator().forEachRemaining(t -> addUnchecked(t, t.hashCode()));
        }

    @Override protected Triple[] newKeyArray( int size )
        { return new Triple[size]; }

    protected int findSlotBySameValueAs( Triple key, Predicate<Triple> predicate )
        {
        final int hash = key.hashCode();
        int index = initialIndexFor( hash );
        while (true)
            {
            final Triple current = keys[index];
            if (current == null) return index;
            if (hash == hashes[index] && predicate.test( current )) return ~index;
            if (--index < 0) index += keys.length;
            }
        }

    @Override
    public boolean containsBySameValueAs( Triple t, Predicate<Triple> predicate )
        { return findSlotBySameValueAs( t, predicate ) < 0; }

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
        { return keys.length; }

    @Override
    public boolean isHashed() { return true; }

    @Override
    public boolean add( Triple t, int hashCode )
        {
        final var slot = findSlot( t, hashCode );
        if (slot < 0) return false;
        keys[slot] = t;
        hashes[slot] = hashCode;
        if (++size > threshold) grow();
        return true;
        }

    @Override
    public void addUnchecked( Triple t, int hashCode )
        {
        final int slot = findSlot( t, hashCode );
        keys[slot] = t;
        hashes[slot] = hashCode;
        if (++size > threshold) grow();
        }

    @Override
    public boolean add( Triple t ) { throw new UnsupportedOperationException(); }

    @Override
    public void addUnchecked( Triple t ) { throw new UnsupportedOperationException(); }

    protected void grow()
        {
        final Triple [] oldContents = keys;
        final int [] oldHashes = hashes;
        keys = new Triple[calcGrownCapacityAndSetThreshold()];
        hashes = new int[keys.length];
        for (int i = 0; i < oldContents.length; i += 1)
            {
            Triple t = oldContents[i];
            if (t != null)
                {
                final int slot = findSlot( t, oldHashes[i] );
                keys[slot] = t;
                hashes[slot] = oldHashes[i];
                }
            }
        }

    @Override
    public boolean remove( Triple t, int hashCode )
        {
            return super.tryRemoveKey( t, hashCode );
        }

    @Override
    public void removeUnchecked( Triple t, int hashCode )
        {
            super.removeKey( t, hashCode );
        }

    @Override
    public boolean remove( Triple t ) { throw new UnsupportedOperationException(); }

    @Override
    public void removeUnchecked( Triple t ) { throw new UnsupportedOperationException(); }

    @Override
    public ExtendedIterator<Triple> iterator()
        { return super.keyIterator(); }

    @Override public Spliterator<Triple> spliterator()
        { return super.keySpliterator(); }
    }
