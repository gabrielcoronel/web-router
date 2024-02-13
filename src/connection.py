import json
import urllib.parse # parse_qs

class Connection:
    """
    Class for representing a generic connection regardless of the internal
    implementation

    A connection defines a mechanism for sending events to a client
    """

    def __init__(self, websocket):
        """
        Initializes the connection internals

        In this case, it stores a websocket object from the 'websockets'
        library
        """

        self._websocket = websocket

    def get_entity_key(self):
        """
        Retrieves the entity key required for registration of this connection,
        from the internal representation

        In case a valid entity key cannot be retrieved (e.g it was not provided
        by the client), a KeyError exception is raised.
        """

        try:
            connection_query_string_starting_index = self._websocket.path.index("?")
        except ValueError:
            raise KeyError

        connection_query_string = self._websocket.path[connection_query_string_starting_index + 1:]
        connection_query_parameters = urllib.parse.parse_qs(connection_query_string)

        try:
            entity_key = connection_query_parameters["entityKey"][0]
        except KeyError:
            raise

        return entity_key

    async def send_event(self, event):
        """
        Sends an event to the client
        """

        payload = json.dumps(event)

        await self._websocket.send(payload)

    async def receive_action(self):
        """
        Receives an action from the client

        Runs a first partial validation on the action object. It check that
        the received action object contains a 'type' property, if not, a
        KeyError exception is raise.
        """

        payload = await self._websocket.recv()
        action = json.loads(payload)

        if "type" not in action:
            raise KeyError

        return action
