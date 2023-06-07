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
       final Node node = getIndexNode( t );

       TripleBunch s = bunchMap.get( node );
       if (s == null)
           {
           bunchMap.put(node, s = new ArrayBunch());
           s.addUnchecked( t );
           size++;
           return true;
           }

       if (s.isHashed())
           {
           if(s.add( t, hashCode ))
               {
               size++;
               return true;
               }
           return false;
           }
        else if (s.size() == 9)
           {
               bunchMap.put( node, s = new HashedTripleBunch( s ) );
               if(s.add( t, hashCode ))
                   {
                   size++;
                   return true;
                   }
               return false;
           }
        else
          {
               if(s.add( t ))
               {
                   size++;
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
            final Node node = getIndexNode( t );

            // Feb 2016 : no measurable difference.
            //TripleBunch s = bunchMap.getOrSet(o, (k)->new ArrayBunch()) ;

            TripleBunch s = bunchMap.get( node );
            if (s == null)
                {
                bunchMap.put( node, s = new ArrayBunch() );
                s.addUnchecked( t );
                }
            else if (s.isHashed())
                {
                s.addUnchecked(t, hashCode);
                }
            else if (s.size() == 9)
                {
                bunchMap.put( node, s = new HashedTripleBunch( s ) );
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
       final Node node = getIndexNode( t );
       final TripleBunch s = bunchMap.get( node );

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
           if (s.size() == 0) bunchMap.remove( node );
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
            final Node node = getIndexNode( t );
            final TripleBunch s = bunchMap.get( node );

            if (s == null)
                return;

            if (s.isHashed())
                s.removeUnchecked( t, hashCode );
            else
                s.removeUnchecked( t );

            size--;
            if (s.size() == 0) bunchMap.remove( node );
        }

    /**
        Answer an iterator over all the triples in this NTM which have index node
        <code>o</code>.
    */
    @Override public ExtendedIterator<Triple> iterator( Node node )
       {
       TripleBunch s = bunchMap.get( node );
       return s == null ? NullIterator.<Triple>instance() : s.iterator();
       }

    @Override public boolean containsBySameValueAs( Triple t )
       { 
       final TripleBunch s = bunchMap.get( getIndexNode( t ) );

       if(s == null) return false;

       return s.containsBySameValueAs( t,
               triple ->
                       t.getPredicate().equals(triple.getPredicate())
                    && t.getObject().equals(triple.getObject()));
       }
    
    /**
        Answer an iterator over all the triples in this NTM which match
        <code>pattern</code>. The index field of this NTM is guaranteed
        concrete in the pattern.
    */
    @Override public ExtendedIterator<Triple> iterator( Node index, Node n2, Node n3 )
       {
       final TripleBunch s = bunchMap.get( index );

       if (s == null) return NullIterator.<Triple>instance();

       final var filter = FieldFilter.filterOn(f2, n2, f3, n3);
       return filter.hasFilter()
           ? s.iterator().filterKeep( filter.getFilter() )
           : s.iterator();
       }

        protected TripleBunch get( Node index )
        { return bunchMap.get( index ); }
    }
