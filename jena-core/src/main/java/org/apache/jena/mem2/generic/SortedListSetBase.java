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

package org.apache.jena.mem2.generic;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * ArrayList which is sorted when the size reaches a given size.
 * @param <E>
 */
public abstract class SortedListSetBase<E> extends ArrayList<E> implements Set<E> {

    protected int getSizeToStartSorting() {
        return DEFAULT_SIZE_TO_START_SORTING;
    }

    protected abstract Comparator<E> getComparator();

    private static int DEFAULT_SIZE_TO_START_SORTING = 15;

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public SortedListSetBase(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Constructs a list containing the elements of the specified
     * collection, in the order they are returned by the collection's
     * iterator.
     *
     * @param c the collection whose elements are to be placed into this list
     * @throws NullPointerException if the specified collection is null
     */
    public SortedListSetBase(Collection<? extends E> c) {
        super(c);
        var comparator = this.getComparator();
        if(comparator != null) {
            super.sort(this.getComparator());
        }
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public SortedListSetBase() {
        super();
    }


    protected Predicate<E> getContainsPredicate(final E value) {
        return other -> value.equals(other);
    }

    /**
     * Returns {@code true} if this list contains the specified element.
     * More formally, returns {@code true} if and only if this list contains
     * at least one element {@code e} such that
     * {@code Objects.equals(o, e)}.
     *
     * @param o element whose presence in this list is to be tested -> o must be of type <E> and not null.
     * @return {@code true} if this list contains the specified element
     */
    @Override
    public boolean contains(Object o) {
        var e = (E) o;
        var predicate = getContainsPredicate(e);
        for (E e1 : this) {
            if(predicate.test(e1)) {
                return true;
            }
        }
//        if(this.size() < this.getSizeToStartSorting()) {
//            for (E e1 : this) {
//                if(predicate.test(e1)) {
//                    return true;
//                }
//            }
//        } else {
//            var startIndex = Collections.binarySearch(this, e, this.getComparator());
//            if(startIndex < 0) {
//                return false;
//            } else if (predicate.test(super.get(startIndex))) {
//                return true;
//            } else {
//                var lowerIndex = startIndex;
//                var upperIndex = startIndex;
//                while (0 <= lowerIndex || upperIndex < super.size()) {
//                    if(0 <= --lowerIndex) {
//                        if (predicate.test(super.get(lowerIndex))) {
//                            return true;
//                        }
//                    }
//                    if(++upperIndex < super.size()) {
//                        if (predicate.test(super.get(upperIndex))) {
//                            return true;
//                        }
//                    }
//                }
//            }
//        }
        return false;
    }

    /**
     * Appends the specified element to the end of this list.
     *
     * @param e element to be appended to this list
     * @return {@code true} (as specified by {@link Collection#add})
     */
    @Override
    public boolean add(E e) {
        if(this.size() < this.getSizeToStartSorting()) {
            if (super.contains(e)) {
                return false; /*triple already exists*/
            }
            super.add(e);
            if(this.size() == this.getSizeToStartSorting()) {
                super.sort(this.getComparator());
            }
        } else {
            var index = Collections.binarySearch(this, e, getComparator());
            // < 0 if element is not in the list, see Collections.binarySearch
            if (index < 0) {
                super.add(~index, e);
            }
            else {
                E e1;
                /*search forward*/
                for (var i = index; i < super.size(); i++) {
                    if (e.equals(e1 = super.get(i))) {
                        return false;
                    }
                    if (0 != getComparator().compare(e, e1)) {
                        break;
                    }
                }
                if(index > 0) {
                    /*search backward*/
                    for (var i = index-1; i >= 0; i--) {
                        if (e.equals(e1 = super.get(i))) {
                            return false;
                        }
                        if (0 != getComparator().compare(e, e1)) {
                            break;
                        }
                    }
                }
                // Insertion index is index of existing element, to add new element
                // behind it increase index+
                super.add(index, e);
            }

        }
        return true;
    }

    public void addUnsafe(E e) {
        if(this.size() < this.getSizeToStartSorting()) {
            super.add(e);
            if(this.size() == this.getSizeToStartSorting()) {
                super.sort(this.getComparator());
            }
        } else {
            var index = Collections.binarySearch(this, e, getComparator());
            // < 0 if element is not in the list, see Collections.binarySearch
            if (index < 0) {
                super.add(~index, e);
            }
            else {
                // Insertion index is index of existing element, to add new element
                // behind it increase index
                super.add(index, e);
            }
        }
    }

    /**
     * Removes the first occurrence of the specified element from this list,
     * if it is present.  If the list does not contain the element, it is
     * unchanged.  More formally, removes the element with the lowest index
     * {@code i} such that
     * {@code Objects.equals(o, get(i))}
     * (if such an element exists).  Returns {@code true} if this list
     * contained the specified element (or equivalently, if this list
     * changed as a result of the call).
     *
     * @param o element to be removed from this list, if present
     * @return {@code true} if this list contained the specified element
     */
    @Override
    public boolean remove(Object o) {
        var elementToRemove = (E)o;
        if(this.size() < this.getSizeToStartSorting()) {
            return super.remove(o);
        } else {
            var index = Collections.binarySearch(this, elementToRemove, getComparator());
            // < 0 if element is not in the list, see Collections.binarySearch
            if (index < 0) {
                return false;
            } else {
                E e;
                /*search forward*/
                for (var i = index; i < super.size(); i++) {
                    if (elementToRemove.equals(e = super.get(i))) {
                        super.remove(i);
                        return true;
                    }
                    if (0 != getComparator().compare(elementToRemove, e)) {
                        break;
                    }
                }
                if (index > 0) {
                    /*search backward*/
                    index--;
                    for (var i = index; i >= 0; i--) {
                        if (elementToRemove.equals(e = super.get(i))) {
                            super.remove(i);
                            return true;
                        }
                        if (0 != getComparator().compare(elementToRemove, e)) {
                            break;
                        }
                    }
                }
            }
        }
        return false;
    }

    public void removeUnsafe(E elementToRemove) {
        if(this.size() < this.getSizeToStartSorting()) {
            super.remove(elementToRemove);
        } else {
            var index = Collections.binarySearch(this, elementToRemove, getComparator());
            E e;
            /*search forward*/
            for (var i = index; i < super.size(); i++) {
                if (elementToRemove.equals(e = super.get(i))) {
                    super.remove(i);
                    return;
                }
                if (0 != getComparator().compare(elementToRemove, e)) {
                    break;
                }
            }
            if (index > 0) {
                /*search backward*/
                for (var i = index-1; i >= 0; i--) {
                    if (elementToRemove.equals(e = super.get(i))) {
                        super.remove(i);
                        return;
                    }
                    if (0 != getComparator().compare(elementToRemove, e)) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Inserts the specified element at the specified position in this
     * list. Shifts the element currently at that position (if any) and
     * any subsequent elements to the right (adds one to their indices).
     *
     * @param index   index at which the specified element is to be inserted
     * @param element element to be inserted
     * @throws IndexOutOfBoundsException {@inheritDoc}
     */
    @Override
    public void add(int index, E element) {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes the element at the specified position in this list.
     * Shifts any subsequent elements to the left (subtracts one from their
     * indices).
     *
     * @param index the index of the element to be removed
     * @return the element that was removed from the list
     * @throws IndexOutOfBoundsException {@inheritDoc}
     */
    @Override
    public E remove(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * Appends all of the elements in the specified collection to the end of
     * this list, in the order that they are returned by the
     * specified collection's Iterator.  The behavior of this operation is
     * undefined if the specified collection is modified while the operation
     * is in progress.  (This implies that the behavior of this call is
     * undefined if the specified collection is this list, and this
     * list is nonempty.)
     *
     * @param c collection containing elements to be added to this list
     * @return {@code true} if this list changed as a result of the call
     * @throws NullPointerException if the specified collection is null
     */
    @Override
    public boolean addAll(Collection<? extends E> c) {
        var modified = super.addAll(c);
        super.sort(getComparator());
        return modified;
    }

    /**
     * Inserts all of the elements in the specified collection into this
     * list, starting at the specified position.  Shifts the element
     * currently at that position (if any) and any subsequent elements to
     * the right (increases their indices).  The new elements will appear
     * in the list in the order that they are returned by the
     * specified collection's iterator.
     *
     * @param index index at which to insert the first element from the
     *              specified collection
     * @param c     collection containing elements to be added to this list
     * @return {@code true} if this list changed as a result of the call
     * @throws IndexOutOfBoundsException {@inheritDoc}
     * @throws NullPointerException      if the specified collection is null
     */
    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes from this list all of the elements whose index is between
     * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.
     * Shifts any succeeding elements to the left (reduces their index).
     * This call shortens the list by {@code (toIndex - fromIndex)} elements.
     * (If {@code toIndex==fromIndex}, this operation has no effect.)
     *
     * @param fromIndex
     * @param toIndex
     * @throws IndexOutOfBoundsException if {@code fromIndex} or
     *                                   {@code toIndex} is out of range
     *                                   ({@code fromIndex < 0 ||
     *                                   toIndex > size() ||
     *                                   toIndex < fromIndex})
     */
    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes from this list all of its elements that are contained in the
     * specified collection.
     *
     * @param c collection containing elements to be removed from this list
     * @return {@code true} if this list changed as a result of the call
     * @throws ClassCastException   if the class of an element of this list
     *                              is incompatible with the specified collection
     *                              (<a href="Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if this list contains a null element and the
     *                              specified collection does not permit null elements
     *                              (<a href="Collection.html#optional-restrictions">optional</a>),
     *                              or if the specified collection is null
     * @see Collection#contains(Object)
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Retains only the elements in this list that are contained in the
     * specified collection.  In other words, removes from this list all
     * of its elements that are not contained in the specified collection.
     *
     * @param c collection containing elements to be retained in this list
     * @return {@code true} if this list changed as a result of the call
     * @throws ClassCastException   if the class of an element of this list
     *                              is incompatible with the specified collection
     *                              (<a href="Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if this list contains a null element and the
     *                              specified collection does not permit null elements
     *                              (<a href="Collection.html#optional-restrictions">optional</a>),
     *                              or if the specified collection is null
     * @see Collection#contains(Object)
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param filter
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(Comparator<? super E> c) {
        if(c != getComparator()) {
            throw new UnsupportedOperationException();
        }
        super.sort(getComparator());
    }
}