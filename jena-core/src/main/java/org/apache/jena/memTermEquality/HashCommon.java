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

import org.apache.jena.shared.BrokenException;
import org.apache.jena.shared.JenaException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.Consumer;

/**
    Shared stuff for our hashing implementations: does the base work for
    hashing and growth sizes.
*/
public abstract class HashCommon<Key>
    {
    /**
        Jeremy suggests, from his experiments, that load factors more than
        0.6 leave the table too dense, and little advantage is gained below 0.4.
        Although that was with a quadratic probe, I'm borrowing the same 
        plausible range, and use 0.5 by default. 
    */
    protected static final double loadFactor = 0.5;
    
    /**
        The keys of whatever table it is we're implementing. Since we share code
        for triple sets and for node->bunch maps, it has to be an Object array; we
        take the casting hit.
     */
    protected Key [] keys;

    /**
        Hashes of the keys, stored separately because we need to use them for
        resizing and more.
     */
    protected int [] hashes;
    
    /**
        The threshold number of elements above which we resize the table;
        equal to the capacity times the load factor.
    */
    protected int threshold;
    
    /**
        The number of active elements in the table, maintained incrementally.
    */
    protected int size = 0;
    
    /**
        Initialise this hashed thingy to have <code>initialCapacity</code> as its
        capacity and the corresponding threshold. All the key elements start out
        null.
    */
    protected HashCommon( int initialCapacity )
        {
        keys = newKeyArray( initialCapacity );
        hashes = new int[keys.length];
        threshold = (int) (keys.length * loadFactor);
        }
    
    /**
        Subclasses must implement to answer a new Key[size] array.
    */
    protected abstract Key[] newKeyArray( int size );

    /**
        When removeFrom [or remove] removes a key, it calls this method to 
        remove any associated values, passing in the index of the key's slot. 
        Subclasses override if they have any associated values.
    */
    protected void removeAssociatedValues( int here )
        {}

    /**
        When removeFrom [or remove] moves a key, it calls this method to move 
        any associated values, passing in the index of the slot <code>here</code>
        to move to and the index of the slot <code>scan</code> to move from.
        Subclasses override if they have any associated values.
    */
    protected void moveAssociatedValues( int here, int scan )
        {}
    
    /**
        Answer the item at index <code>i</code> of <code>keys</code>. This
        method is for testing purposes <i>only</i>.
    */
    public Key getItemForTestingAt( int i )
        { return keys[i]; }
    
    /**
        Answer the initial index for the object <code>key</code> in the table.
        With luck, this will be the final position for that object. The initial index
        will always be non-negative and less than <code>capacity</code>.
    <p>
        Implementation note: do <i>not</i> use <code>Math.abs</code> to turn a
        hashcode into a positive value; there is a single specific integer on which
        it does not work. (Hence, here, the use of bitmasks.)
    */
    protected final int initialIndexFor( int hashOfKey )
        { return (improveHashCode( hashOfKey ) & 0x7fffffff) % keys.length; }

    /**
        Answer the transformed hash code, intended to be an improvement
        on the objects own hashcode. The magic number 127 is performance
        voodoo to (try to) eliminate problems experienced by Wolfgang.
    */
    protected int improveHashCode( int hashCode )
        { return hashCode * 127; }    
    
    /**
        Search for the slot in which <code>key</code> is found. If it is absent,
        return the index of the free slot in which it could be placed. If it is present,
        return the bitwise complement of the index of the slot it appears in. Hence
        negative values imply present, positive absent, and there's no confusion
        around 0.
    */
    protected final int findSlot( Key key, int hashCodeOfKey )
        {
        int index = initialIndexFor( hashCodeOfKey );
        while (true)
            {
            Key current = keys[index];
            if (current == null) return index; 
            if (hashCodeOfKey == hashes[index] && key.equals( current )) return ~index;
            if (--index < 0) index += keys.length;
            }
        }   

    /**
        Remove the object <code>key</code> from this hash's keys if it
        is present (if it's absent, do nothing). If a key is removed, the
        <code>removeAssociatedValues</code> will be invoked. If a key
        is moved, the <code>moveAssociatedValues</code> method will
        be called.
    */
    public void removeKey( Key key, int hashCodeOfKey )
        {
        int slot = findSlot( key, hashCodeOfKey );
        if (slot < 0) removeFrom( ~slot );
        }

    /**
     Remove the object <code>key</code> from this hash's keys if it
     is present (if it's absent, do nothing). If a key is removed, the
     <code>removeAssociatedValues</code> will be invoked. If a key
     is moved, the <code>moveAssociatedValues</code> method will
     be called.
     */
    public boolean tryRemoveKey( Key key, int hashCodeOfKey )
        {
            int slot = findSlot( key, hashCodeOfKey );
            if (slot < 0)
                {
                removeFrom( ~slot );
                return true;
                }
            return false;
        }

    private void primitiveRemove( Key key, int hashCodeOfKey )
        {
            removeKey( key, hashCodeOfKey );
        }

    /**
        Work out the capacity and threshold sizes for a new improved bigger
        table (bigger by a factor of two, at present).
    */
    protected int calcGrownCapacityAndSetThreshold()
        {
        final var capacity = nextSize( keys.length * 2 );
        threshold = (int) (capacity * loadFactor);
        return capacity;
        }
     
    // Hash tables are 0.25 to 0.5 full so these numbers
    // are for storing about 1/3 of that number of items. 
    // The larger sizes are added so that the system has "soft failure"
    // rather implying guaranteed performance. 
    // https://primes.utm.edu/lists/small/millions/
    static final int [] primes =
        {
        7, 19, 37, 79, 149, 307, 617, 1237, 2477, 4957, 9923,
        19_853, 39_709, 79_423, 158_849, 317_701, 635_413,
        1_270_849, 2_541_701, 5_083_423
        , 10_166_857
        , 20_333_759   
        , 40_667_527
        , 81_335_047
        , 162_670_111
        , 325_340_233
        , 650_680_469
        , 982_451_653 // 50 millionth prime - Largest at primes.utm.edu.
        };
    
    protected static int nextSize(int atLeast) {
        for ( int prime : primes ) {
            if ( prime > atLeast )
                return prime;
        }
        //return atLeast ;        // Input is 2*current capacity.
        // There are some very large numbers in the primes table. 
        throw new JenaException("Failed to find a 'next size': atleast = "+atLeast) ; 
    }
    
    /**
        Remove the triple at element <code>i</code> of <code>contents</code>.
        This is an implementation of Knuth's Algorithm R from tAoCP vol3, p 527,
        with exchanging of the roles of i and j so that they can be usefully renamed
        to <i>here</i> and <i>scan</i>.
    <p>
        It relies on linear probing but doesn't require a distinguished REMOVED
        value. Since we resize the table when it gets fullish, we don't worry [much]
        about the overhead of the linear probing.
    <p>
        Iterators running over the keys may miss elements that are moved from the
        bottom of the table to the top because of Iterator::remove. removeFrom
        returns such a moved key as its result, and null otherwise.
    */
    protected Key removeFrom( int here )
        {
        final int original = here;
        Key wrappedAround = null;
        size -= 1;
        while (true)
            {
            keys[here] = null;
            removeAssociatedValues( here );
            int scan = here;
            while (true)
                {
                if (--scan < 0) scan += keys.length;
                if (keys[scan] == null) return wrappedAround;
                int r = initialIndexFor( hashes[scan] );
                if (scan <= r && r < here || r < here && here < scan || here < scan && scan <= r)
                    { /* Nothing. We'd have preferred an `unless` statement. */}
                else
                    {
                    if (here >= original && scan < original)
                        { wrappedAround = keys[scan]; }
                    keys[here] = keys[scan];
                    hashes[here] = hashes[scan];
                    moveAssociatedValues( here, scan );
                    here = scan;
                    break;
                    }
                }
            }
        }    
    
    void showkeys()
        {
        if (false)
            {
            System.err.print( ">> KEYS:" );
            for (int i = 0; i < keys.length; i += 1)
                if (keys[i] != null) System.err.print( " " + initialIndexFor( hashes[i] ) + "@" + i + "::" + keys[i] );
            System.err.println();
            }
        }

    public ExtendedIterator<Key> keyIterator()
        {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
            { if (size != initialSize) throw new ConcurrentModificationException(); };
        return new SparseArrayIterator<>(keys, checkForConcurrentModification);
        }
    


    public Spliterator<Key> keySpliterator()
        {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
            { if (size != initialSize) throw new ConcurrentModificationException(); };
        return new SparseArraySpliterator<>(keys, checkForConcurrentModification);
        }
    }
