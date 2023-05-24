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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Function;

/**
    An implementation of BunchMap that does open-addressed hashing.
*/
public abstract class HashedBunchMap extends HashCommon<TripleBunch, Object> implements BunchMap
    {
    public HashedBunchMap()
        {
        super( 10 );
        }

    protected abstract Object getIndexingValue(Triple triple);

    @Override protected Object mapValueToKey(TripleBunch tripleBunch)
        { return tripleBunch.getIndexingValue(); }

    @Override protected TripleBunch[] newValueArray(int size )
        { return new TripleBunch[size]; }

    @Override protected int findSlot(final Object key, final int hashCodeOfKey )
        {
        int index = initialIndexFor( hashCodeOfKey );
        while (true)
            {
            final TripleBunch current = values[index];
            if (current == null) return index;
            if (hashCodeOfKey == hashes[index] && key.equals( current.getIndexingValue() )) return ~index;
            if (--index < 0) index += values.length;
            }
        }

    @Override protected void grow()
        {
        final TripleBunch [] oldContents = values;
        final int [] oldHashes = hashes;
        final TripleBunch [] newValues = values = newValueArray(calcGrownCapacityAndSetThreshold());
        final int [] newHashes = hashes = new int[values.length];
        for (int i = 0; i < oldContents.length; i += 1)
            {
            if (null != oldContents[i])
                {
                final int slot = findSlot( oldContents[i].getIndexingValue(), oldHashes[i] );
                newValues[slot] = oldContents[i];
                newHashes[slot] = oldHashes[i];
                }
            }
        }

    /**
        Clear this map: all entries are removed. The keys <i>and value</i> array 
        elements are set to null (so the values may be garbage-collected).
    */
    @Override
    public void clear()
        {
        size = 0;
        for (int i = 0; i < values.length; i += 1) values[i] = null;
        }  
    
    @Override
    public long size()
        { return size; }
        
    @Override
    public TripleBunch get(Object key )
        {
        int slot = findSlot( key, key.hashCode() );
        return slot < 0 ? values[~slot] : null;
        }

    @Override
    public void put( Object key, TripleBunch value )
        {
        final int hashCodeOfKey = key.hashCode();
        int slot = findSlot( key, hashCodeOfKey );
        if (slot < 0)
            {
            values[~slot] = value;
            }
        else
            put$(slot, hashCodeOfKey, value) ;
        }

    @Override
    public TripleBunch getOrSet(Object key, Function<Object, TripleBunch> setter) {
        final int hashCodeOfKey = key.hashCode();
        int slot = findSlot( key, hashCodeOfKey );
        if (slot < 0)
            // Get.
            return values[~slot] ;
        // Or set value.
        TripleBunch value = setter.apply(key) ;
        put$(slot, hashCodeOfKey, value) ;
        return value ;
        }
    
    private void put$(int slot, int hashCodeOfKey, TripleBunch value) {
        hashes[slot] = hashCodeOfKey;
        values[slot] = value;
        size++;
        if ( size == threshold )
            grow();
    }

    @Override public void remove(Object key)
        { super.removeKey( key, key.hashCode() ); }


    @Override public Iterator<TripleBunch> iterator()
        { return super.valueIterator(); }

    @Override public Spliterator<TripleBunch> spliterator() {
        final var initialChanges = changes;
        final Runnable checkForConcurrentModification = () ->
        {
            if (changes != initialChanges) throw new ConcurrentModificationException();
        };

        return new SparseArraySpliterator<>(values, size, checkForConcurrentModification);
        }
    }
