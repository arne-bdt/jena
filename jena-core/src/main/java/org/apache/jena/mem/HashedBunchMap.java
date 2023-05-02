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

package org.apache.jena.mem;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function ;

import org.apache.jena.shared.BrokenException ;

/**
    An implementation of BunchMap that does open-addressed hashing.
*/
public class HashedBunchMap extends HashCommon<Object> implements BunchMap
    {
    protected TripleBunch [] values;
    
    public HashedBunchMap()
        {
        super( 10 );
        values = new TripleBunch[capacity];
        }

    @Override protected Object[] newKeyArray( int size )
        { return new Object[size]; }
    
    /**
        Clear this map: all entries are removed. The keys <i>and value</i> array 
        elements are set to null (so the values may be garbage-collected).
    */
    @Override
    public void clear()
        {
        size = 0;
        for (int i = 0; i < capacity; i += 1) keys[i] = values[i] = null; 
        }  
    
    @Override
    public long size()
        { return size; }
        
    @Override
    public TripleBunch get( Object key )
        {
        int slot = findSlot( key );
        return slot < 0 ? values[~slot] : null;
        }

    @Override
    public void put( Object key, TripleBunch value )
        {
        int slot = findSlot( key );
        if (slot < 0)
            values[~slot] = value;
        else
            put$(slot, key, value) ;
        }

    @Override
    public TripleBunch getOrSet( Object key, Function<Object, TripleBunch> setter) {
        int slot = findSlot( key );
        if (slot < 0)
            // Get.
            return values[~slot] ;
        // Or set value.
        TripleBunch value = setter.apply(key) ;
        put$(slot, key, value) ;
        return value ;
        }
    
    private void put$(int slot, Object key, TripleBunch value) {
        keys[slot] = key;
        values[slot] = value;
        size += 1;
        if ( size == threshold )
            grow();
    }
    
    protected void grow()
        {
        Object [] oldContents = keys;
        TripleBunch [] oldValues = values;
        final int oldCapacity = capacity;
        growCapacityAndThreshold();
        keys = newKeyArray( capacity );
        values = new TripleBunch[capacity];
        for (int i = 0; i < oldCapacity; i += 1)
            {
            Object key = oldContents[i];
            if (key != null) 
                {
                int j = findSlot( key );
                if (j < 0) 
                    {
                    throw new BrokenException( "oh dear, already have a slot for " + key  + ", viz " + ~j );
                    }
                keys[j] = key;
                values[j] = oldValues[i];
                }
            }
        }

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
        return new Iterator<TripleBunch>()
            {
            final int initialChanges = changes;
            int pos = capacity-1;

            @Override public boolean hasNext()
                {
                while(-1 < pos)
                    {
                        if(null != values[pos]) return true;
                        pos--;
                    }
                return false;
                }

            @Override public TripleBunch next()
                {
                if (changes > initialChanges) throw new ConcurrentModificationException();
                if (-1 < pos && null != values[pos]) return values[pos--];
                throw new NoSuchElementException();
                }

            @Override public void forEachRemaining(Consumer<? super TripleBunch> action)
                {
                while(-1 < pos)
                    {
                    if(null != values[pos]) action.accept(values[pos]);
                    pos--;
                    }
                if (changes > initialChanges) throw new ConcurrentModificationException();
                }

            @Override
                public void remove() {
                    if (changes > initialChanges) throw new ConcurrentModificationException();
                    HashedBunchMap.super.removeFrom(pos + 1);
                }
            };
        }

    @Override public Spliterator<TripleBunch> spliterator()
        {
        return new Spliterator<TripleBunch>()
            {
            final int initialChanges = changes;
            private int pos = capacity;
            private int remaining = size;
            private boolean hasBeenSplit = false;

            @Override public boolean tryAdvance(Consumer<? super TripleBunch> action)
                {
                if (changes > initialChanges) throw new ConcurrentModificationException();
                while (0 < pos)
                    {
                    if(null != values[--pos])
                        {
                        remaining--;
                        action.accept(values[pos]);
                        return true;
                        }
                    }
                return false;
                }
            @Override public void forEachRemaining(Consumer<? super TripleBunch> action)
                {
                while (0 < pos)
                    {
                    if(null != values[--pos])
                        {
                        action.accept(values[pos]);
                        }
                    }
                remaining = 0;
                if (changes > initialChanges) throw new ConcurrentModificationException();
                }
            @Override public Spliterator<TripleBunch> trySplit()
                {
                if (remaining < 2 || this.pos < 2) return null;
                final var toIndex = this.pos;
                this.pos = (this.pos >>> 1);
                this.remaining = remaining >>> 1;
                this.hasBeenSplit = true;
                return new ArrayWithNullsSubSpliteratorUnSized(this.pos, toIndex, remaining, initialChanges);
                }

            @Override public long estimateSize() {
                return remaining;
            }

            @Override public int characteristics()
                {
                return this.hasBeenSplit
                        ? DISTINCT | NONNULL | IMMUTABLE
                        : DISTINCT | NONNULL | IMMUTABLE | SIZED;
                }
            };
        }

        final class ArrayWithNullsSubSpliteratorUnSized implements Spliterator<TripleBunch> {

            final int initialChanges;
            private final int fromIndex;
            private int pos;
            private int estimatedSize;

            /**
             * Create a spliterator for the given array, with the given size.
             * @param fromIndex the index of the first element, inclusive
             * @param toIndex   the index of the last element, exclusive
             * @param estimatedSize the estimated size
             */
            public ArrayWithNullsSubSpliteratorUnSized(final int fromIndex, final int toIndex, final int estimatedSize, final int initialChanges) {
                this.fromIndex = fromIndex;
                this.pos = toIndex;
                this.estimatedSize = estimatedSize;
                this.initialChanges = initialChanges;
            }

             @Override
            public boolean tryAdvance(Consumer<? super TripleBunch> action) {
                 if (changes > initialChanges) throw new ConcurrentModificationException();
                 while (fromIndex < pos) {
                    if(null != values[--pos]) {
                        estimatedSize--;
                        action.accept(values[pos]);
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void forEachRemaining(Consumer<? super TripleBunch> action) {
                while (fromIndex < pos) {
                    if(null != values[--pos]) {
                        action.accept(values[pos]);
                    }
                }
                estimatedSize = 0;
                if (changes > initialChanges) throw new ConcurrentModificationException();
            }


            @Override
            public Spliterator<TripleBunch> trySplit() {
                var entriesCount = pos - fromIndex;
                if (entriesCount < 2) {
                    return null;
                }
                final var toIndexOfSubIterator = this.pos;
                this.pos = fromIndex + (entriesCount >>> 1);
                this.estimatedSize = estimatedSize >>> 1;
                return new ArrayWithNullsSubSpliteratorUnSized(this.pos, toIndexOfSubIterator, estimatedSize, initialChanges);
            }

            @Override
            public long estimateSize() {
                return estimatedSize;
            }

            @Override
            public int characteristics() {
                return DISTINCT | NONNULL | IMMUTABLE;
            }
        }


    }
