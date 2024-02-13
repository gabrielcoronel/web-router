import json # dumps, loads
import asyncio # Future
import websockets # serve
import urllib.parse # parse_qs
from connection import Connection
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

        try:
            entity_key = connection.get_entity_key()
        except KeyError:
            await connection.send_event({
                "status": "error",
                "type": "entityKeyMissingAtConnectionRegistration"
            })

            raise CloseConnection

        connection_key = self._connection_registry.register(entity_key, connection)

        await connection.send_event({
            "status": "success",
            "type": "connectionRegistered",
            "data": {
                "connectionKey": connection_key
            }
        })

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
            await connection.send_event({
                "status": "error",
                "type": "missingConnectionKeyAtConnectionUnregistration"
            })

            return

        try:
            self._connection_registry.unregister(entity_key, connection_key)
        except ConnectionNotFoundError:
            await connection.send_event({
                "status": "error",
                "type": "invalidConnectionKey",
                "data": {
                    "connectionKey": connection_key
                }
            })

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
            await connection.send_event({
                "status": "error",
                "type": "missingDataAtBroadcasting"
            })

            return

        try:
            await self._connection_registry.broadcast(entity_key, data)
        except EntityNotFoundError:
            await connection.send_event({
                "status": "error",
                "type": "invalidEntityKey",
                "data": {
                    "entityKey": entity_key
                }
            })

            return
        except ConnectionSendingError:
            await connection.send_event({
                "status": "error",
                "type": "sendingFailed",
                "data": {
                    "entityKey": entity_key,
                    "data": data
                }
            })

            return

    async def _handle_action(self, action, entity_key, connection):
        """
        Dispatches an action to its appropriate handler

        The requested action is specified via a string that is meant to be
        parsed as JSON specifying the action metadata. At this stage of the
        execution of the action, the action metadata is expected to contain
        a valid 'type' field, hence it's validated as such.

        If the action type is valid, the action is dispatched to its
        corresponding handler.
        """

        match action["type"]:
            case "unregister":
                await self._handle_unregister(action, entity_key, connection)

            case "broadcast":
                await self._handle_broadcast(action, entity_key, connection)

            case _:
                await connection.send_event({
                    "status": "error",
                    "type": "invalidAction",
                    "data": {
                        "action": action
                    }
                })

    async def _handle_connection(self, websocket):
        """
        Handles registration of an arriving connection and dispatches actions
        to their respective handlers

        This method is called by 'websockets' whenever a new client connects
        to the Websocket server. Therefore handles registration related tasks
        and dispatches the client's requested actions (messages) to the
        appropriate handlers.
        """

        connection = Connection(websocket)

        try:
            entity_key = await self._handle_registration(connection)
        except CloseConnection:
            return

        while True:
            try:
                action = await connection.receive_action()
            except KeyError:
                await connection.send_event({
                    "status": "error",
                    "type": "invalidAction",
                    "data": {
                        "action": action
                    }
                })

            await self._handle_action(action, entity_key, connection)

    async def serve(self, port):
        """
        Starts a Websocket server in which the router lives in

        The router operates through this server via the _handle_connection
        method. _handle_collection is called by 'websockets' library whenever a new
        client connects to the server.

        This method depends on the websockets library, in case another library
        is used for setting up websocket servers, this method should be
        rewritten.
        """

        async with websockets.serve(self._handle_connection, "", port):
            await asyncio.Future()
