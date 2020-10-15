/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.crdt;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import org.apache.gossip.crdt.OrSet.Builder.Operation;

/*
 * A immutable set 
 */
public class OrSet<E>  implements CrdtAddRemoveSet<E, Set<E>, OrSet<E>> {

  /**
   * 集合元素，可以为实际的值，KEY 数据item， item对相应的UUID，每个数据对应一个UUID
   */
  private final Map<E, Set<UUID>> elements = new HashMap<>();
  /**
   *集合失效元素
   */
  private final Map<E, Set<UUID>> tombstones = new HashMap<>();
  /**
   * 集合有效值
   */
  private final transient Set<E> val;
  
  public OrSet(){
    val = computeValue();
  }
  
  OrSet(Map<E, Set<UUID>> elements, Map<E, Set<UUID>> tombstones){
    this.elements.putAll(elements);
    this.tombstones.putAll(tombstones);
    val = computeValue();
  }
  
  @SafeVarargs
  public OrSet(E ... elements){
    this(new HashSet<>(Arrays.asList(elements)));
  }

  /**
   * 添加元素集
   * @param elements
   */
  public OrSet(Set<E> elements) {
    for (E e: elements){
      internalAdd(e);
    }
    val = computeValue();
  }
  
  public OrSet(Builder<E>builder){
    for (Builder<E>.OrSetElement<E> e: builder.elements){
      if (e.operation == Operation.ADD){
        internalAdd(e.element);
      } else {
        internalRemove(e.element);
      }
    }
    val = computeValue();
  }
  
  /**
   * This constructor is the way to remove elements from an existing set
   *
   * @param set
   * @param builder 
   */
  public OrSet(OrSet<E> set, Builder<E> builder){
    elements.putAll(set.elements);
    tombstones.putAll(set.tombstones);
    for (Builder<E>.OrSetElement<E> e: builder.elements){
      if (e.operation == Operation.ADD){
        internalAdd(e.element);
      } else {
        internalRemove(e.element);
      }
    }
    val = computeValue();
  }

  /**
   * 合并集合
   * @param a
   * @param b
   * @return
   */
  static Set<UUID> mergeSets(Set<UUID> a, Set<UUID> b) {
    if ((a == null || a.isEmpty()) && (b == null || b.isEmpty())) {
      return null;
    }
    Set<UUID> res = new HashSet<>(a);
    res.addAll(b);
    return res;
  }

  /**
   * 内部合并数据集
   * @param map
   * @param key
   * @param value  移除给定元素的UUID
   */
  private void internalSetMerge(Map<E, Set<UUID>> map, E key, Set<UUID> value) {
    if (value == null) {
      return;
    }
    map.merge(key, value, OrSet::mergeSets);
  }

  /**
   * 合并set集
   * @param left
   * @param right
   */
  public OrSet(OrSet<E> left, OrSet<E> right){
    BiConsumer<Map<E, Set<UUID>>, Map<E, Set<UUID>>> internalMerge = (items, other) -> {
      for (Entry<E, Set<UUID>> l : other.entrySet()){
        internalSetMerge(items, l.getKey(), l.getValue());
      }
    };

    internalMerge.accept(elements, left.elements);
    internalMerge.accept(elements, right.elements);
    internalMerge.accept(tombstones, left.tombstones);
    internalMerge.accept(tombstones, right.tombstones);

    val = computeValue();
  }

  /**
   * @param e
   * @return
   */
  @Override
  public OrSet<E> add(E e) {
    return this.merge(new OrSet<>(e));
  }

  /**
   * @param e
   * @return
   */
  @Override
  public OrSet<E> remove(E e) {
    return new OrSet<>(this, new Builder<E>().remove(e));
  }

  public OrSet.Builder<E> builder(){
    return new OrSet.Builder<>();
  }
  
  @Override
  public OrSet<E> merge(OrSet<E> other) {
    return new OrSet<E>(this, other);
  }

  /**
   * 内部添加元素
   * @param element
   */
  private void internalAdd(E element) {
    Set<UUID> toMerge = new HashSet<>();
    toMerge.add(UUID.randomUUID());
    internalSetMerge(elements, element, toMerge);
  }

  /**
   * @param element
   */
  private void internalRemove(E element){
    internalSetMerge(tombstones, element, elements.get(element));
  }

  /*
   * Computes the live values by analyzing the elements and tombstones
   * 根据集合元素与失效元素，计算存活的元素
   */
  private Set<E> computeValue(){
    Set<E> values = new HashSet<>();
    for (Entry<E, Set<UUID>> entry: elements.entrySet()){
      Set<UUID> deleteIds = tombstones.get(entry.getKey());
      // if not all tokens for current element are in tombstones
      //如果失效的元素的UUID不存在或者不包括元素的UUID集，则元素有效
      if (deleteIds == null || !deleteIds.containsAll(entry.getValue())) {
        values.add(entry.getKey());
      }
    }
    return values;
  }
  
  @Override
  public Set<E> value() {
    return val;
  }

  @Override
  public OrSet<E> optimize() {
    return this;
  }
  
  public static class Builder<E> {
    /**
     * 支持增加和移除操作
     */
    public static enum Operation {
      ADD, REMOVE
    };

    /**
     * 集合元素
     * @param <EL>
     */
    private class OrSetElement<EL> {
      EL element;
      Operation operation;

      private OrSetElement(EL element, Operation operation) {
        this.element = element;
        this.operation = operation;
      }
    }

    private List<OrSetElement<E>> elements = new ArrayList<>();

    public Builder<E> add(E element) {
      elements.add(new OrSetElement<E>(element, Operation.ADD));
      return this;
    }

    public Builder<E> remove(E element) {
      elements.add(new OrSetElement<E>(element, Operation.REMOVE));
      return this;
    }

    public Builder<E> mutate(E element, Operation operation) {
      elements.add(new OrSetElement<E>(element, operation));
      return this;
    }
  }

  
  public int size() {
    return value().size();
  }

  
  public boolean isEmpty() {
    return value().size() == 0;
  }

  
  public boolean contains(Object o) {
    return value().contains(o);
  }

  
  public Iterator<E> iterator() {
    Iterator<E> managed = value().iterator();
    return new Iterator<E>() {

      @Override
      public void remove() {
        throw new IllegalArgumentException();
      }

      @Override
      public boolean hasNext() {
        return managed.hasNext();
      }

      @Override
      public E next() {
        return managed.next();
      }
      
    };
  }

  public Object[] toArray() {
    return value().toArray();
  }

  public <T> T[] toArray(T[] a) {
    return value().toArray(a);
  }

  public boolean containsAll(Collection<?> c) {
    return this.value().containsAll(c);
  }

  public boolean addAll(Collection<? extends E> c) {
    throw new IllegalArgumentException();
  }

  public boolean retainAll(Collection<?> c) {
    throw new IllegalArgumentException();
  }

  public boolean removeAll(Collection<?> c) {
    throw new IllegalArgumentException();
  }

  public void clear() {
    throw new IllegalArgumentException();
  }

  @Override
  public String toString() {
    return "OrSet [elements=" + elements + ", tombstones=" + tombstones + "]" ;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((value() == null) ? 0 : value().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("rawtypes")
    OrSet other = (OrSet) obj;
    if (elements == null) {
      if (other.elements != null)
        return false;
    } else if (!value().equals(other.value()))
      return false;
    return true;
  }

  Map<E, Set<UUID>> getElements() {
    return elements;
  }

  Map<E, Set<UUID>> getTombstones() {
    return tombstones;
  }

}
