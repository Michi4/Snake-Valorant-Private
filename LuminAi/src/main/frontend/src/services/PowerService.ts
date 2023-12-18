import { store } from "@/store/Store.ts";

const socket = new WebSocket(`ws://${process.env.BACKEND_BASE_URL}/subscribeUpdates`);

export function startSocketClient() {
    socket.onopen = function (event) {
        console.log("WebSocket connection opened:", event);
    };

    socket.onmessage = function (event) {
        store.deviceData = JSON.parse(event.data);
    };

    socket.onclose = function (event) {
        console.log("WebSocket connection closed:", event);
    };

    socket.onerror = function (error) {
        console.error("WebSocket error:", error);
    };
}