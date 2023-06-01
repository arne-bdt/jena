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

import org.apache.jena.graph.Node;
import org.apache.jena.shared.BrokenException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
    An implementation of BunchMap that does open-addressed hashing.
*/
public class HashedBunchMap extends HashCommon<Node> implements BunchMap
    {
    protected TripleBunch[] values;
    
    public HashedBunchMap()
        {
        super( 10 );
        values = new TripleBunch[keys.length];
        }

    @Override protected Node[] newKeyArray( int size )
        { return new Node[size]; }
    
    /**
        Clear this map: all entries are removed. The keys <i>and value</i> array 
        elements are set to null (so the values may be garbage-collected).
    */
    @Override
    public void clear()
        {
        size = 0;
        for (int i = 0; i < keys.length; i += 1)
            {
            keys[i] = null;
            values[i] = null;
            }
        }  
    
    @Override
    public long size()
        { return size; }
        
    @Override
    public TripleBunch get(Node key )
        {
        final int slot = findSlot( key, key.hashCode() );
        return slot < 0 ? values[~slot] : null;
        }

    @Override
    public void put( Node key, TripleBunch value )
        {
        final int hashCodeOfKey = key.hashCode();
        final int slot = findSlot( key, hashCodeOfKey );
        if (slot < 0)
            {
            values[~slot] = value;
            }
        else
            put$(slot, key, hashCodeOfKey, value) ;
        }

    @Override
    public TripleBunch getOrSet(Node key, Function<Node, TripleBunch> setter) {
        final int hashCodeOfKey = key.hashCode();
        final int slot = findSlot( key, hashCodeOfKey );
        if (slot < 0)
            // Get.
            return values[~slot] ;
        // Or set value.
        TripleBunch value = setter.apply(key) ;
        put$(slot, key, hashCodeOfKey, value) ;
        return value ;
        }
    
    private void put$(int slot, Node key, int hashCodeOfKey, TripleBunch value) {
        keys[slot] = key;
        hashes[slot] = hashCodeOfKey;
        values[slot] = value;
        size++;
        if ( size == threshold )
            grow();
    }
    
    protected void grow()
        {
        final Node [] oldContents = keys;
        final int[] oldHashes = hashes;
        final TripleBunch[] oldValues = values;
        keys = newKeyArray( calcGrownCapacityAndSetThreshold() );
        hashes = new int[keys.length];
        values = new TripleBunch[keys.length];
        for (int i = 0; i < oldContents.length; i += 1)
            {
            if (oldContents[i] != null)
                {
                final int j = findSlot( oldContents[i], oldHashes[i] );
                if (j < 0) 
                    {
                    throw new BrokenException( "oh dear, already have a slot for " + oldContents[i]  + ", viz " + ~j );
                    }
                keys[j] = oldContents[i];
                hashes[j] = oldHashes[i];
                values[j] = oldValues[i];
                }
            }
        }

    @Override public void remove(Node key)
        { super.removeKey( key, key.hashCode() ); }

        /**
        Called by HashCommon when a key is removed: remove
        associated element of the <code>values</code> array.
    */
    @Override protected void removeAssociatedValues( int here )
        { values[here] = null; }
    
    /**
        Called by HashCommon when a key is moved: move the
        associated element of the <code>values</code> array.
    */
    @Override protected void moveAssociatedValues( int here, int scan )
        { values[here] = values[scan]; }

    @Override public Iterator<TripleBunch> iterator()
        {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
            { if (size != initialSize) throw new ConcurrentModificationException(); };
        return new SparseArrayIterator<>(values, checkForConcurrentModification);
        }

    @Override public Spliterator<TripleBunch> spliterator()
        {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
            { if (size != initialSize) throw new ConcurrentModificationException(); };
        return new SparseArraySpliterator<>(values, size, checkForConcurrentModification);
        }
    }
