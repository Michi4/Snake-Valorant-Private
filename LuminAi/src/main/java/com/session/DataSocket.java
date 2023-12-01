package com.session;

import com.model.Data;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.util.HashSet;
import java.util.Set;


@ServerEndpoint("/subscribeUpdates")
@ApplicationScoped
public class DataSocket {
    Set<Session> sessions = new HashSet<>();

    @OnOpen
    public void onOpen(Session session) {
        sessions.add(session);
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        sessions.remove(session);
        throw new RuntimeException("error with user websocket", throwable);
    }

    @OnClose
    public void onClose(Session session) {
        sessions.remove(session);
    }

    public void publish(Data data) {
        sessions.forEach(session -> session.getAsyncRemote().sendObject(data, result -> {
            if (result.getException() != null) {
                throw new RuntimeException("error with user websocket", result.getException());
            }
        }));
    }
}