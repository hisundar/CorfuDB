package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * crossbach 5/21/15
 * borrowed map method implementations from Michael's CDBSimpleMap
 */
public class CDBLogicalOrderedMap<K extends Comparable,V> implements ICorfuDBObject<CDBSimpleMap<K,V>>, Map<K,V>
{
    transient ISMREngine<TreeMap> smr;
    ITransaction tx;
    UUID streamID;

    /**
     * ctor
     * @param map
     * @param tx
     */
    public CDBLogicalOrderedMap(CDBLogicalOrderedMap<K,V> map, ITransaction tx) {
        this.streamID = map.streamID;
        this.tx = tx;
    }

    /**
     * ctor
     * @param stream
     * @param smrClass
     */
    @SuppressWarnings("unchecked")
    public CDBLogicalOrderedMap(IStream stream, Class<? extends ISMREngine> smrClass) {
        try {
            streamID = stream.getStreamID();
            smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, TreeMap.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ctor
     * @param stream
     */
    public CDBLogicalOrderedMap(IStream stream) {
        streamID = stream.getStreamID();
        smr = new SimpleSMREngine<TreeMap>(stream, TreeMap.class);
    }

    /**
     * get the key range containing <tt>records</tt> keys starting
     * at the key <tt>start</tt> (or closest value above it).
     * @param start start key
     * @param records number of records
     * @return a set with the specified number of entries
     */
    public Set<K> getKeyRange(K start, int records) {
        return (Set<K>) accessorHelper((ISMREngineCommand<TreeMap<K, V>>) (map, opts) -> {
            Set<K> result = new HashSet<K>();
            if (records != 0) {
                int count = 0;
                K actualstart = start;
                if (!map.containsKey(start)) {
                    actualstart = map.higherKey(start);
                    if (actualstart == null)
                        throw new RuntimeException("invalid range parameter!");
                }
                K current = actualstart;
                do {
                    result.add(current);
                    current = map.higherKey(current);
                    count++;
                } while (count < records && current != null);
            }
            opts.getReturnResult().complete(result);
        });
    }

    /**
     * get the range containing <tt>records</tt> entries starting
     * at the key <tt>start</tt> (or closest value above it).
     * @param start start key
     * @param records number of records
     * @return a map with the specified number of entries
     */
    public SortedMap<K, V> getRange(K start, int records) {
        return (SortedMap<K,V>) accessorHelper((ISMREngineCommand<TreeMap<K, V>>) (map, opts) -> {
            SortedMap<K,V> result = new TreeMap<K, V>();
            if(records != 0) {
                int count = 0;
                K actualstart = start;
                if (!map.containsKey(start)) {
                    actualstart = map.higherKey(start);
                    if (actualstart == null)
                        throw new RuntimeException("invalid range parameter!");
                }
                K current = actualstart;
                if (current != null) {
                    do {
                        result.put(current, map.get(current));
                        current = map.higherKey(current);
                        count++;
                    } while (count < records && current != null);
                }
            }
            opts.getReturnResult().complete(result);
        });
    }

    /**
     * Returns the number of key-value mappings in this map.  If the
     * map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        return (int) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.size());
        });
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    @Override
    public boolean isEmpty() {
        return (boolean) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.isEmpty());
        });
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified
     * key.  More formally, returns <tt>true</tt> if and only if
     * this map contains a mapping for a key <tt>k</tt> such that
     * <tt>(key==null ? k==null : key.equals(k))</tt>.  (There can be
     * at most one such mapping.)
     *
     * @param key key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *                              does not permit null keys
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    public boolean containsKey(Object key) {
        return (boolean) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.containsKey(key));
        });
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value.  More formally, returns <tt>true</tt> if and only if
     * this map contains at least one mapping to a value <tt>v</tt> such that
     * <tt>(value==null ? v==null : value.equals(v))</tt>.  This operation
     * will probably require time linear in the map size for most
     * implementations of the <tt>Map</tt> interface.
     *
     * @param value value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the
     * specified value
     * @throws ClassCastException   if the value is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified value is null and this
     *                              map does not permit null values
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    public boolean containsValue(Object value) {
        return (boolean) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.containsValue(value));
        });
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p>
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     * <p>
     * <p>If this map permits null values, then a return value of
     * {@code null} does not <i>necessarily</i> indicate that the map
     * contains no mapping for the key; it's also possible that the map
     * explicitly maps the key to {@code null}.  The {@link #containsKey
     * containsKey} operation may be used to distinguish these two cases.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or
     * {@code null} if this map contains no mapping for the key
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *                              does not permit null keys
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return (V) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.get(key));
        });
    }

    /**
     * Associates the specified value with the specified key in this map
     * (optional operation).  If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * <tt>true</tt>.)
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * (A <tt>null</tt> return can also indicate that the map
     * previously associated <tt>null</tt> with <tt>key</tt>,
     * if the implementation supports <tt>null</tt> values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of the specified key or value
     *                                       prevents it from being stored in this map
     * @throws NullPointerException          if the specified key or value is null
     *                                       and this map does not permit null keys or values
     * @throws IllegalArgumentException      if some property of the specified key
     *                                       or value prevents it from being stored in this map
     */
    @Override
    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        return (V) mutatorAccessorHelper(
                (ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
                    opts.getReturnResult().complete(map.put(key, value));
                }
        );
    }

    /**
     * Removes the mapping for a key from this map if it is present
     * (optional operation).   More formally, if this map contains a mapping
     * from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
     * is removed.  (The map can contain at most one such mapping.)
     * <p>
     * <p>Returns the value to which this map previously associated the key,
     * or <tt>null</tt> if the map contained no mapping for the key.
     * <p>
     * <p>If this map permits null values, then a return value of
     * <tt>null</tt> does not <i>necessarily</i> indicate that the map
     * contained no mapping for the key; it's also possible that the map
     * explicitly mapped the key to <tt>null</tt>.
     * <p>
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the key is of an inappropriate type for
     *                                       this map
     *                                       (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified key is null and this
     *                                       map does not permit null keys
     *                                       (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return (V) mutatorAccessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.remove(key));
        });
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * (optional operation).  The effect of this call is equivalent to that
     * of calling {@link #put(Object, Object) put(k, v)} on this map once
     * for each mapping from key <tt>k</tt> to value <tt>v</tt> in the
     * specified map.  The behavior of this operation is undefined if the
     * specified map is modified while the operation is in progress.
     *
     * @param m mappings to be stored in this map
     * @throws UnsupportedOperationException if the <tt>putAll</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of a key or value in the
     *                                       specified map prevents it from being stored in this map
     * @throws NullPointerException          if the specified map is null, or if
     *                                       this map does not permit null keys or values, and the
     *                                       specified map contains null keys or values
     * @throws IllegalArgumentException      if some property of a key or value in
     *                                       the specified map prevents it from being stored in this map
     */
    @Override
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends K, ? extends V> m) {
        mutatorHelper((ISMREngineCommand<ConcurrentHashMap>) (map,opts) -> {
            map.putAll(m);
        });
    }

    /**
     * Removes all of the mappings from this map (optional operation).
     * The map will be empty after this call returns.
     *
     * @throws UnsupportedOperationException if the <tt>clear</tt> operation
     *                                       is not supported by this map
     */
    @Override
    public void clear() {
        mutatorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            map.clear();
        });
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     *
     * @return a set view of the keys contained in this map
     */
    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        return (Set<K>) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.keySet());
        });
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a collection view of the values contained in this map
     */
    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        return (Collection<V>) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.values());
        });
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation, or through the
     * <tt>setValue</tt> operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
     * <tt>clear</tt> operations.  It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a set view of the mappings contained in this map
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return (Set<Entry<K,V>>) accessorHelper((ISMREngineCommand<ConcurrentHashMap>) (map, opts) -> {
            opts.getReturnResult().complete(map.entrySet());
        });
    }

    /**
     * Get the type of the underlying object
     */
    @Override
    public Class<?> getUnderlyingType() {
        return TreeMap.class;
    }

    /**
     * Get the UUID of the underlying stream
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }

    /**
     * Get underlying SMR engine
     *
     * @return The SMR engine this object was instantiated under.
     */
    @Override
    public ISMREngine getUnderlyingSMREngine() {
        return smr;
    }

    /**
     * Set underlying SMR engine
     *
     * @param engine
     */
    @Override
    @SuppressWarnings("unchecked")
    public void setUnderlyingSMREngine(ISMREngine engine) {
        this.smr = engine;
    }

    /**
     * Set the stream ID
     *
     * @param streamID The stream ID to set.
     */
    @Override
    public void setStreamID(UUID streamID) {
        this.streamID = streamID;
    }

    /**
     * Get the underlying transaction
     *
     * @return The transaction this object is currently participating in.
     */
    @Override
    public ITransaction getUnderlyingTransaction() {
        return tx;
    }
}

