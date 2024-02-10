import json # dumps, loads
import asyncio # Future
import websockets # serve
import urllib.parse # parse_qs
from connection_registry import (
    ConnectionRegistry,
    ConnectionNotFoundError,
    ConnectionSendingError,
    EntityNotFoundError
)


class CloseConnection(Exception):
    """
    Exception raised to signal that a connection should be closed
    """

    pass


def get_connection_query_parameters(connection):
    """
    Retrieves the query parameters used to connect through a Websocket from
    a connection object
    """

    try:
        connection_query_string_starting_index = connection.path.index("?")
    except ValueError:
        return dict()

    connection_query_string = connection.path[connection_query_string_starting_index:]
    connection_query_parameters = urllib.parse.parse_qs(connection_query_string)

    return connection_query_parameters


async def send_event_through_connection(event, connection):
    """
    Sends an event (dictionary) through a connection object
    """

    payload = json.dumps(event)

    await connection.send(payload)


class Router:
    """
    Class for encapsulating the router dependencies

    A Router internally holds a private connection registry in order to
    route messages from and to clients. It simply add a validation and
    notification layer on top of the connection registry. So that clients
    can listen to error or success events, and their requested actions are
    validated and processed properly.
    """

    def __init__(self):
        """
        Initializes the internal router dependencies
        """

        self._connection_registry = ConnectionRegistry()

    async def _handle_registration(self, connection):
        """
        Handles the registration of a new connection into the connection
        registry

        The entity key required for registration is parsed and grabbed from
        the query string used when establishing the Websocket connection.

        In case of a missing entity key, an appropiate error event is sent back
        to the client. In case of a succesful registration, the client is
        notified through a success event.
        """

        connection_query_parameters = get_connection_query_parameters(connection)

        try:
            entity_key = connection_query_parameters["entityKey"]
        except KeyError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "entityKeyMissingAtConnectionRegistration"
                },
                connection
            )

            raise CloseConnection

        connection_key = self._connection_registry.register(entity_key, connection)

        await send_event_through_connection(
            {
                "status": "success",
                "type": "connectionRegistered",
                "data": {
                    "connectionKey": connection_key
                }
            },
            connection
        )

        return entity_key

    async def _handle_unregister(self, action, entity_key, connection):
        """
        Handles the unregister action on the connection registry

        Validates that a connection key is present on a 'data' field. If the
        unregistration process fails because the connection wasn't found
        (the connection key is not valid), a error event is sent back to the
        client.
        """

        try:
            connection_key = action["data"]["connection_key"]
        except KeyError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "missingConnectionKeyAtConnectionUnregistration"
                },
                connection
            )

            return

        try:
            self._connection_registry.unregister(entity_key, connection_key)
        except ConnectionNotFoundError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "invalidConnectionKey",
                    "data": {
                        "connectionKey": connection_key
                    }
                },
                connection
            )

            return

    async def _handle_broadcast(self, action, entity_key, connection):
        """
        Handles the broadcasting action on the connection registry

        A 'data' field is expected in the action metadata, therefore it's
        validated as such. Broadcasting of the given 'data' field is attempted.
        However this data is wrapped in a success event so the receiving clients
        can distinguish from other events. Any error is forwarded to the client
        as an error event.
        """

        try:
            data = action["data"]
        except KeyError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "missingDataAtBroadcasting"
                },
                connection
            )

            return

        try:
            self._connection_registry.broadcast(
                entity_key,
                {
                    "status": "success",
                    "type": "broadcastedData",
                    "data": data
                }
            )
        except EntityNotFoundError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "invalidEntityKey",
                    "data": {
                        "entityKey": entity_key
                    }
                },
                connection
            )

            return
        except ConnectionSendingError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "sendingFailed",
                    "data": {
                        "entityKey": entity_key,
                        "data": data
                    }
                },
                connection
            )

            return

    async def _handle_action(self, action_string, entity_key, connection):
        """
        Dispatches an action to its appropriate handler

        The requested action is specified via a string that is meant to be
        parsed as JSON specifying the action metadata. At this stage of the
        execution of the action, the action metadata is expected to contain
        a valid 'type' field, hence it's validated as such.

        If the action type is valid, the action is dispatched to its
        corresponding handler.
        """

        action = json.loads(action_string)

        try:
            action_type = action["type"]
        except KeyError:
            await send_event_through_connection(
                {
                    "status": "error",
                    "type": "invalidAction",
                    "data": {
                        "action": action
                    }
                },
                connection
            )

        match action_type:
            case "unregister":
                await self._handle_unregister(action, entity_key, connection)

            case "broadcast":
                await self._handle_broadcast(action, entity_key, connection)

            case _:
                await send_event_through_connection(
                    {
                        "status": "error",
                        "type": "invalidAction",
                        "data": {
                            "action": action
                        }
                    },
                    connection
                )

    async def _handle_connection(self, connection):
        """
        Handles registration of an arriving connection and dispatches actions
        to their respective handlers

        This method is called by 'websockets' whenever a new client connects
        to the Websocket server. Therefore handles registration related tasks
        and dispatches the client's requested actions (messages) to the
        appropriate handlers.
        """

        try:
            entity_key = await self._handle_registration(connection)
        except CloseConnection:
            return

        async for action_string in connection:
            await self._handle_action(action_string, entity_key, connection)

    async def serve(self, port):
        """
        Starts a Websocket server in which the router lives in

        The router operates through this server via the _handler_connection
        method. This method is called by 'websockets' library whenever a new
        client connects to the server.
        """

        async with websockets.serve(self._handle_connection, "", port):
            await asyncio.Future()
