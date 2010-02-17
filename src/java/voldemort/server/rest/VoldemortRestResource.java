package voldemort.server.rest;

import com.sun.jersey.api.NotFoundException;
import voldemort.client.LocalClient;
import voldemort.client.StoreClient;
import voldemort.server.StoreRepository;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

/**
 * Rest Resource, is exposed by jersey.
 *
 * Currently handles GET and PUT of string:string.
 */
@Path("/api/{storeName}/{key}")
public class VoldemortRestResource {
    @Context
    ServletContext context;

    /**
     * Handles GET requests to /api/[storename]/[keyname]. Returns a String representation
     * of the data in the store.
     *
     * @param storeName name of the store.
     * @param key the key to look up.
     * @return the value.
     */
    @GET
	@Produces("text/plain")
	public String getEntryAsText(@PathParam("storeName") String storeName,
                                 @PathParam("key") String key) {

        StoreClient<ByteArray, byte[]> client = getClient(storeName);
        byte [] value = client.getValue(new ByteArray(key.getBytes()));
        if(value == null) {
            throw new NotFoundException("Could not find key ["+key+"]");
        }
        return new String(value);

	}

    @PUT
    @Consumes("text/plain")
    public void putTextEntry(@PathParam("storeName") String storeName,
                             @PathParam("key") String key,
                             String value) {        
        StoreClient<ByteArray, byte[]> client = getClient(storeName);
        client.put(new ByteArray(key.getBytes()), value.getBytes());
    }
    
    StoreClient<ByteArray, byte[]> getClient(String storeName) {
        StoreRepository storeRepository = (StoreRepository) context.getAttribute(RestService.REPOSITORY_KEY);
        Store<ByteArray, byte[]> store = storeRepository.getLocalStore(storeName);
        if(store == null) {
            throw new NotFoundException("Could not find store "+storeName);
        }
        return new LocalClient<ByteArray, byte[]>(store);
    }

}
