import collections # deque
from concurrent.futures import ProcessPoolExecutor


def unique_numeric_key_generator():
    """
    Genarator function of unique numeric identifiers
    """

    count = 1

    while True:
        yield count

        count += 1


class ConnectionNotFoundError(Exception):
    """
    Exception raised when a connection was expected to be found but was not
    in the connection register
    """

    pass


class EntityNotFoundError(Exception):
    """
    Exception raised when an entity was expected to be found but was not
    in the connection register
    """

    pass

class ConnectionSendingError(Exception):
    """
    Exception raised when there was an internal error while trying to send data
    through a connection
    """

    pass


class ConnectionRegistry:
    """
    Class for representing a connection registry

    A connection registry is responsible for globally bookkeeping all the
    Websocket connections in the application and operating on them.

    One can register and unregister connections under an application entity
    unique key, as well as broadcasting a piece of data on all the connections
    registered under a certain entity key.
    """

    def __init__(self):
        """
        Initializes the connection registry

        Internally, the registry is represented with a dictionary that maps
        unique entity keys to double ended linked lists (deque) holding such
        entity connections. Also, a unique numeric key generator function is
        initialized for assigning a unique key to each connection, this is done
        for bookkeeping purposes.
        """

        self._registry = dict()
        self._generate_unique_numeric_key = unique_numeric_key_generator()

    def register(self, entity_key, connection):
        """
        Registers a given connection under a unique entity key

        If entity key is new for the connection registry, a new key-deque pair
        will be created to register the given connection. Otherwise, the given
        connection will be inserted into the existing deque for that specific
        entity key.

        Internally, a unique numeric key is assigned to the new connection. This
        key is returned to the calling code so it can later unregister this same
        connection.
        """

        new_connection_key = self._generate_unique_numeric_key()

        if entity_key in self._registry:
            entity_connection_list = self._registry[entity_key]
            new_connection_entry = tuple(connection_key, connection)
            entity_connection_list.append(new_connection_entry)
        else:
            new_connection_entry = tuple(connection_key, connection)
            new_entity_connection_list = collections.deque([new_connection_entry])
            self._registry[entity_key] = new_entity_collection_list

        return new_connection_key

    def unregister(self, entity_key, connection_key):
        """
        Unregisters the connection registered under the given entity key which
        was assigned to the given connection key

        If such connection does not exist, ConnectionNotFoundError is raised.

        On the other hand, if such connection is the last one registered under
        the entity key, the key-deque pair for than entity key is not destroyed
        and instead kept inside the connection registry. This is done for
        caching purposes for two main reasons.

        1. Entity key insertion can be skipped in case another connection will
        be registered under that entity key, which is likely.
        2. Because the corresponding deque is not destroyed, advantage can be
        taken from the deque internal memory block caching mechanism.
        """

        try:
            entity_connection_list = self._registry[entity_key]
        except KeyError:
            raise ConnectionNotFoundError

        if len(entity_connection_list) == 0:
            raise ConnectionNotFoundError

        connection_to_unregister_index = None

        for index, connection_entry in enumerate(entity_connection_list):
            if connection_entry[0] == connection_key:
                connection_to_unregister_index = index
                break

        if connection_to_unregister_index is None:
            raise ConnectionNotFoundError

        del entity_connection_list[connection_to_unregister_index]

    def broadcast(self, entity_key, data):
        """
        Broadcasts the given piece of data on all the connections register under
        the given entity key

        Broadcasting is done concurrently using multiprocessing for efficiency
        reasons. This is convinient since the sending client, in principle,
        is not interested in knowing when the data is received. Instead, the
        receiving client can subscribe to when the data arrives independently,
        regardless of when the multiprocessing executor decided to send the
        data.

        If sending the data raise a exception, this exception is forwarded
        through a ConnectionSendingError.
        """

        try:
            entity_connection_list = self._registry[entity_key]
        except KeyError:
            raise EntityNotFoundError

        connections_to_broadcast = (
            connection
            for _, connection in entity_connection_list
        )

        with ProcessPoolExecutor() as executor:
            try:
                executor.map(
                    lambda connection: connection.send(data),
                    connections_to_broadcast
                )
            except Exception as exception:
                raise ConnectionSendingError(exception)
