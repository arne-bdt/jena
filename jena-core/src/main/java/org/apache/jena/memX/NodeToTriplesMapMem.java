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

package org.apache.jena.memX;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.Triple.Field;
import org.apache.jena.shared.JenaException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.function.Predicate;

public class NodeToTriplesMapMem extends NodeToTriplesMapBase
    {    
    public NodeToTriplesMapMem( Field indexField, Field f2, Field f3 )
       { super( indexField, f2, f3 ); }
    
    /**
        Add <code>t</code> to this NTM; the node <code>o</code> <i>must</i>
        be the index node of the triple. Answer <code>true</code> iff the triple
        was not previously in the set, ie, it really truly has been added.
    */
    @Override public boolean add( Triple t, int hashCode )
       {
       Object o = getIndexField( t );

       // Feb 2016 : no measurable difference.
       //TripleBunch s = bunchMap.getOrSet(o, (k)->new ArrayBunch()) ;

       TripleBunch s = bunchMap.get( o );
       if (s == null)
           {
           bunchMap.put(o, s = new ArrayBunch());
           s.addUnchecked( t );
           size++;
           return true;
           }

       if (s.isHashed())
           {
           if(s.add( t, hashCode ))
               {
               size += 1;
               return true;
               }
           return false;
           }
        else if (s.size() == 9)
           {
               bunchMap.put( o, s = new HashedTripleBunch( s ) );
               if(s.add( t, hashCode ))
                   {
                   size += 1;
                   return true;
                   }
               return false;
           }
        else
          {
               if(s.add( t ))
               {
                   size += 1;
                   return true;
               }
               return false;
           }
        }

    /**
         Add <code>t</code> to this NTM; the node <code>o</code> <i>must</i>
         be the index node of the triple.
         <code>t</code> must not already be in the set.
         <code>t</code> is added without checking if it is already a member.
    */
    @Override public void addUnchecked( Triple t, int hashCode )
        {
            Object o = getIndexField( t );

            // Feb 2016 : no measurable difference.
            //TripleBunch s = bunchMap.getOrSet(o, (k)->new ArrayBunch()) ;

            TripleBunch s = bunchMap.get( o );
            if (s == null)
                bunchMap.put(o, s = new ArrayBunch());

            if (s.isHashed())
                {
                s.addUnchecked(t, hashCode);
                }
            else if (s.size() == 9)
                {
                bunchMap.put( o, s = new HashedTripleBunch( s ) );
                s.addUnchecked( t, hashCode );
                }
            else
                {
                s.addUnchecked( t );
                }
            size++;
        }

    /**
        Remove <code>t</code> from this NTM. Answer <code>true</code> iff the 
        triple was previously in the set, ie, it really truly has been removed. 
    */
    @Override public boolean remove( Triple t, int hashCode )
       {
       Object o = getIndexField( t );
       TripleBunch s = bunchMap.get( o );

       final boolean removed;
       if (s == null)
           removed = false;
       else if (s.isHashed())
           removed = s.remove( t, hashCode );
       else
           removed = s.remove( t );

       if (removed)
           {
           size--;
           if (s.size() == 0) bunchMap.remove( o );
           }
       return removed;
       }

    /**
         Remove <code>t</code> from this NTM.
         <code>t</code> must already be in the set.
         <code>t</code> is removed without checking if it is a member.
     */
    @Override public void removeUnchecked( Triple t, int hashCode )
        {
            Object o = getIndexField( t );
            TripleBunch s = bunchMap.get( o );

            if (s == null)
                return;

            if (s.isHashed())
                s.removeUnchecked( t, hashCode );
            else
                s.removeUnchecked( t );

            size--;
            if (s.size() == 0) bunchMap.remove( o );
        }

    /**
        Answer an iterator over all the triples in this NTM which have index node
        <code>o</code>.
    */
    @Override public ExtendedIterator<Triple> iterator( Object o, HashCommon.NotifyEmpty container )
       {
       TripleBunch s = bunchMap.get( o );
       return s == null ? NullIterator.<Triple>instance() : s.iterator( container );
       }

    public ExtendedIterator<Triple> iterateAll(Triple pattern)
        {
        Predicate<Triple> filter = indexField.filterOn(pattern)
                .and(f2.filterOn(pattern)).and(f3.filterOn(pattern));
        return iterateAll().filterKeep(filter);
        }
    
    public class NotifyMe implements HashCommon.NotifyEmpty
        {
        protected final Object key;
        
        public NotifyMe( Object key )
            { this.key = key; }
        
        // TODO fix the way this interacts (badly) with iteration and CMEs.
        @Override
        public void emptied()
            { if (false) throw new JenaException( "BOOM" ); /* System.err.println( ">> OOPS" ); */ bunchMap.remove( key ); }
        }

    @Override public boolean containsBySameValueAs( Triple t )
       { 
       TripleBunch s = bunchMap.get( getIndexField( t ) );
       return s == null ? false :  s.containsBySameValueAs( t );
       }
    
    /**
        Answer an iterator over all the triples in this NTM which match
        <code>pattern</code>. The index field of this NTM is guaranteed
        concrete in the pattern.
    */
    @Override public ExtendedIterator<Triple> iterator( Node index, Node n2, Node n3 )
       {
       Object indexValue = index.getIndexingValue();
       TripleBunch s = bunchMap.get( indexValue );
//       System.err.println( ">> ntmf::iterator: " + (s == null ? (Object) "None" : s.getClass()) );
       if (s == null) return NullIterator.<Triple>instance();
           var filter = FieldFilter.filterOn(f2, n2, f3, n3);
           return filter.hasFilter()
               ? s.iterator( new NotifyMe( indexValue ) ).filterKeep( filter.getFilter() )
               : s.iterator( new NotifyMe( indexValue ) );
       }

        protected TripleBunch get( Object index )
        { return bunchMap.get( index ); }
    
    /**
     Answer an iterator over all the triples that are indexed by the item <code>y</code>.
        Note that <code>y</code> need not be a Node (because of indexing values).
    */
    @Override public ExtendedIterator<Triple> iteratorForIndexed( Object y )
        { return get( y ).iterator();  }
    }
