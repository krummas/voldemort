package voldemort.client;

import com.google.common.collect.Maps;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Feb 15, 2010 Time: 8:02:39 PM To change this template use File |
 * Settings | File Templates.
 */
public class LocalClient<K,V> implements StoreClient<K, V> {
    private final Store<K, V> store;

    public LocalClient(Store<K,V> store) {
        this.store = store;
    }
    /**
     * Get the value associated with the given key or null if there is no value associated with this key. This method
     * strips off all version information and is only useful when no further storage operations will be done on this key.
     *
     * @param key The key
     */
    public V getValue(K key) {
        Versioned<V> returned = get(key, null);
        if(returned == null)
            return null;
        else
            return returned.getValue();
    }

    /**
     * Get the value associated with the given key or defaultValue if there is no value associated with the key. This
     * method strips off all version information and is only useful when no further storage operations will be done on this
     * key.
     *
     * @param key          The key for which to fetch the associated value
     * @param defaultValue A value to return if there is no value associated with this key
     * @return Either the value stored for the key or the default value.
     */
    public V getValue(K key, V defaultValue) {
        Versioned<V> versioned = get(key);
        if(versioned == null)
            return defaultValue;
        else
            return versioned.getValue();
    }

    /**
     * Get the versioned value associated with the given key or null if no value is associated with the key.
     *
     * @param key The key for which to fetch the value.
     * @return The versioned value, or null if no value is stored for this key.
     */
    public Versioned<V> get(K key) {
         return get(key, null);
    }

    /**
     * Gets the versioned values associated with the given keys and returns them in a Map of keys to versioned values. Note
     * that the returned map will only contain entries for the keys which have a value associated with them.
     *
     * @param keys The keys for which to fetch the values.
     * @return A Map of keys to versioned values.
     */
    public Map<K, Versioned<V>> getAll(Iterable<K> keys) {
        Map<K, List<Versioned<V>>> items;

        items = store.getAll(keys);

        Map<K, Versioned<V>> result = Maps.newHashMapWithExpectedSize(items.size());

        for(Map.Entry<K, List<Versioned<V>>> mapEntry: items.entrySet()) {
            Versioned<V> value = getItemOrThrow(mapEntry.getKey(), null, mapEntry.getValue());
            result.put(mapEntry.getKey(), value);
        }
        return result;
    }

    /**
     * Get the versioned value associated with the given key or the defaultValue if no value is associated with the key.
     *
     * @param key The key for which to fetch the value.
     * @return The versioned value, or the defaultValue if no value is stored for this key.
     */
    public Versioned<V> get(K key, Versioned<V> defaultValue) {
        List<Versioned<V>> items = store.get(key);
        return getItemOrThrow(key, defaultValue, items);
    }

    /**
     * Associated the given value to the key, clobbering any existing values stored for the key.
     *
     * @param key   The key
     * @param value The value
     */
    public void put(K key, V value) {
        List<Version> versions = store.getVersions(key);
        Versioned<V> versioned;
        if(versions.isEmpty())
            versioned = Versioned.value(value, new VectorClock());
        else if(versions.size() == 1)
            versioned = Versioned.value(value, versions.get(0));
        else {
            versioned = get(key, null);
            if(versioned == null)
                versioned = Versioned.value(value, new VectorClock());
            else
                versioned.setObject(value);
        }
        put(key, versioned);
    }

    /**
     * Put the given Versioned value into the store for the given key if the version is greater to or concurrent with
     * existing values. Throw an ObsoleteVersionException otherwise.
     *
     * @param key       The key
     * @param versioned The value and its versioned
     * @throws voldemort.versioning.ObsoleteVersionException
     *
     */
    public void put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        store.put(key, versioned);
    }

    /**
     * Put the versioned value to the key, ignoring any ObsoleteVersionException that may be thrown
     *
     * @param key       The key
     * @param versioned The versioned value
     * @return true if the put succeeded
     */
    public boolean putIfNotObsolete(K key, Versioned<V> versioned) {
        try {
            put(key, versioned);
            return true;
        } catch(ObsoleteVersionException e) {
            return false;
        }
    }

    /**
     * Apply the given action repeatedly until no ObsoleteVersionException is thrown. This is useful for implementing a
     * read-modify-store loop that could be pre-empted by another concurrent update, and should be repeated until it
     * succeeds.
     *
     * @param action The action to apply. This is meant as a callback for the user to extend to provide their own logic.
     * @return true if the action is successfully applied, false if the 3 attempts all result in ObsoleteVersionException
     */
    public boolean applyUpdate(UpdateAction<K, V> action) {
        return applyUpdate(action, 3);
    }

    /**
     * Apply the given action repeatedly until no ObsoleteVersionException is thrown or maxTries unsuccessful attempts have
     * been made. This is useful for implementing a read-modify-store loop.
     *
     * @param action The action to apply
     * @return true if the action is successfully applied, false if maxTries failed attempts have been made
     */
    public boolean applyUpdate(UpdateAction<K, V> action, int maxTries) {
        boolean success = false;
        try {
            for(int i = 0; i < maxTries; i++) {
                try {
                    action.update(this);
                    success = true;
                    return success;
                } catch(ObsoleteVersionException e) {
                    // ignore for now
                }
            }
        } finally {
            if(!success)
                action.rollback();
        }

        // if we got here we have seen too many ObsoleteVersionExceptions
        // and have rolled back the updates
        return false;
    }

    /**
     * Delete any version of the given key which equal to or less than the current versions
     *
     * @param key The key
     * @return true if anything is deleted
     */
    public boolean delete(K key) {
        Versioned<V> versioned = get(key);
        if(versioned == null)
            return false;
        return delete(key, versioned.getVersion());
    }

    /**
     * Delete the specified version and any prior versions of the given key
     *
     * @param key     The key to delete
     * @param version The version of the key
     * @return true if anything is deleted
     */
    public boolean delete(K key, Version version) {
        return store.delete(key, version);
    }

    /**
     * Returns the list of nodes which should have this key.
     *
     * @param key
     * @return a list of Nodes which should hold this key
     */
    public List<Node> getResponsibleNodes(K key) {
        RoutingStrategy strategy = (RoutingStrategy) store.getCapability(StoreCapabilityType.ROUTING_STRATEGY);
        @SuppressWarnings("unchecked")
        Serializer<K> keySerializer = (Serializer<K>) store.getCapability(StoreCapabilityType.KEY_SERIALIZER);
        return strategy.routeRequest(keySerializer.toBytes(key));
    }

    private Versioned<V> getItemOrThrow(K key, Versioned<V> defaultValue, List<Versioned<V>> items) {
        if(items.size() == 0)
            return defaultValue;
        else if(items.size() == 1)
            return items.get(0);
        else
            throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                + ") = " + items, items);
    }
}
