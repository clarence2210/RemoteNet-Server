const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

// Render provides PORT automatically.
// Local testing uses port 3000.
const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";

// Middleware
app.use(cors());
app.use(express.json());

// Socket.IO configuration
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },

    transports: ["websocket", "polling"],

    allowEIO3: false,
    pingInterval: 25000,
    pingTimeout: 20000,
    maxHttpBufferSize: 1e6
});

// =====================================
// HEALTH CHECK
// =====================================

app.get("/", (req, res) => {
    res.status(200).json({
        app: "RemoteNet Relay Server",
        status: "ONLINE",
        hosting: process.env.RENDER
            ? "Render Cloud"
            : "Local Server",
        transport: "Socket.IO WebSocket",
        timestamp: new Date().toISOString()
    });
});

app.get("/health", (req, res) => {
    res.status(200).json({
        status: "ONLINE",
        connectedClients: io.engine.clientsCount,
        transport: "websocket",
        uptime: process.uptime()
    });
});

// =====================================
// SOCKET.IO CONNECTION
// =====================================

io.on("connection", (socket) => {

    console.log("\n------------------------------");
    console.log("NEW CLIENT CONNECTED");
    console.log("Socket ID:", socket.id);
    console.log("Transport:", socket.conn.transport.name);
    console.log("------------------------------");

    // =================================
    // JOIN ROOM
    // =================================

    socket.on("join-room", async (data) => {

        try {

            const roomId = String(data?.roomId || "").trim();

            const role = String(data?.role || "")
                .trim()
                .toLowerCase();

            // Validate room code
            if (!/^[0-9]{6}$/.test(roomId)) {

                socket.emit("join-error", {
                    message: "Invalid room code"
                });

                return;
            }

            // Validate role
            if (role !== "sender" && role !== "receiver") {

                socket.emit("join-error", {
                    message: "Invalid role"
                });

                return;
            }

            // Leave previous room
            const previousRoom = socket.data.roomId;

            if (previousRoom) {

                socket.to(previousRoom).emit("peer-status", {
                    status: "disconnected"
                });

                await socket.leave(previousRoom);

                socket.data.roomId = null;
                socket.data.role = null;
            }

            // Check duplicate role
            const members = await io.in(roomId).fetchSockets();

            const duplicateRole = members.some((member) => {

                return member.id !== socket.id &&
                    member.data.role === role;

            });

            if (duplicateRole) {

                socket.emit("join-error", {
                    message: `A ${role} is already connected to this room`
                });

                return;
            }

            // Join room
            await socket.join(roomId);

            socket.data.roomId = roomId;
            socket.data.role = role;

            console.log(
                `[JOIN] ${role.toUpperCase()} joined room ${roomId}`
            );

            // Confirm joining
            socket.emit("joined-room", {
                roomId: roomId,
                role: role,
                message: "Successfully joined relay room"
            });

            // Notify other phone
            socket.to(roomId).emit("peer-status", {
                status: "connected",
                role: role
            });

            // Get room members
            const roomMembers = await io.in(roomId).fetchSockets();

            io.to(roomId).emit("room-status", {
                roomId: roomId,
                members: roomMembers.map((member) => ({
                    role: member.data.role
                }))
            });

            console.log(
                `[ROOM] ${roomId} has ${roomMembers.length} client(s)`
            );

        } catch (error) {

            console.error("[JOIN ERROR]", error);

            socket.emit("join-error", {
                message: "Server could not join the room"
            });

        }

    });

    // =================================
    // SEND MESSAGE
    // =================================

    socket.on("send-message", (data) => {

        try {

            const roomId = socket.data.roomId;
            const role = socket.data.role;

            if (!roomId || !role) {

                socket.emit("message-error", {
                    message: "Join a room before sending messages"
                });

                return;
            }

            const message = String(data?.message || "").trim();

            if (!message) {

                socket.emit("message-error", {
                    message: "Message cannot be empty"
                });

                return;
            }

            if (message.length > 4000) {

                socket.emit("message-error", {
                    message: "Message is too long"
                });

                return;
            }

            const messageData = {
                sender: role,
                message: message,
                timestamp: new Date().toISOString()
            };

            console.log(
                `[MESSAGE] ${role.toUpperCase()} -> Room ${roomId}: ${message}`
            );

            // Forward message to other phone
            socket.to(roomId).emit("receive-message", messageData);

            // Confirm sender
            socket.emit("message-sent", {
                status: "success",
                message: "Message forwarded to room"
            });

        } catch (error) {

            console.error("[MESSAGE ERROR]", error);

            socket.emit("message-error", {
                message: "Server failed to send message"
            });

        }

    });

    // =================================
    // DISCONNECT
    // =================================

    socket.on("disconnect", async (reason) => {

        const roomId = socket.data.roomId;
        const role = socket.data.role;

        console.log(
            `[DISCONNECTED] ${socket.id} | Reason: ${reason}`
        );

        if (roomId) {

            socket.to(roomId).emit("peer-status", {
                status: "disconnected",
                role: role
            });

            try {

                const remainingMembers =
                    await io.in(roomId).fetchSockets();

                io.to(roomId).emit("room-status", {
                    roomId: roomId,
                    members: remainingMembers.map((member) => ({
                        role: member.data.role
                    }))
                });

            } catch (error) {

                console.error("[DISCONNECT ERROR]", error);

            }

        }

    });

    // =================================
    // SOCKET ERROR
    // =================================

    socket.on("error", (error) => {

        console.error("[SOCKET ERROR]", error);

    });

});

// =====================================
// ENGINE.IO CONNECTION ERRORS
// =====================================

io.engine.on("connection_error", (error) => {

    console.error("\n[ENGINE.IO CONNECTION ERROR]");

    console.error("Code:", error.code);
    console.error("Message:", error.message);
    console.error("Context:", error.context);

});

// =====================================
// SERVER START
// =====================================

server.listen(PORT, HOST, () => {

    console.log("\n================================");
    console.log("     RemoteNet Relay Server");
    console.log("================================");

    console.log("Status  : ONLINE");
    console.log("Host    :", HOST);
    console.log("Port    :", PORT);

    console.log(
        "Hosting :",
        process.env.RENDER ? "RENDER CLOUD" : "LOCAL COMPUTER"
    );

    console.log("================================\n");

});

// =====================================
// SERVER ERROR HANDLING
// =====================================

server.on("error", (error) => {

    console.error("[SERVER ERROR]", error);

});

// =====================================
// GRACEFUL SHUTDOWN
// =====================================

process.on("SIGTERM", () => {

    console.log("SIGTERM received. Closing server...");

    io.close(() => {

        server.close(() => {

            console.log("RemoteNet server stopped.");

            process.exit(0);

        });

    });

});