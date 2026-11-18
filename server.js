
const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

// Firebase Admin SDK v14 modular imports
const { initializeApp, cert } = require("firebase-admin/app");
const { getAuth } = require("firebase-admin/auth");
const { getDatabase } = require("firebase-admin/database");

// =====================================
// FIREBASE ADMIN INITIALIZATION
// =====================================

try {

    const encodedServiceAccount =
        process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;

    if (!encodedServiceAccount) {
        throw new Error(
            "Firebase service account environment variable is missing"
        );
    }

    const serviceAccount = JSON.parse(
        Buffer.from(encodedServiceAccount, "base64").toString("utf8")
    );

    initializeApp({
        credential: cert(serviceAccount),
        databaseURL:
            "https://remotenet-b4106-default-rtdb.asia-southeast1.firebasedatabase.app"
    });

    console.log(
        "[FIREBASE] Admin SDK initialized successfully"
    );

} catch (error) {

    console.error(
        "[FIREBASE] Initialization failed:",
        error.message
    );

    process.exit(1);
}

const firebaseAuth = getAuth();
const firebaseDb = getDatabase();

// =====================================
// SERVER SETUP
// =====================================

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";

app.use(cors());
app.use(express.json());

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
// FIREBASE SOCKET AUTHENTICATION
// =====================================

io.use(async (socket, next) => {

    try {

        const authorization =
            socket.handshake.headers.authorization || "";

        if (!authorization.startsWith("Bearer ")) {

            return next(
                new Error("Authentication required")
            );
        }

        const idToken = authorization
            .substring(7)
            .trim();

        if (!idToken) {

            return next(
                new Error("Authentication token missing")
            );
        }

        // Verify Firebase ID token and check revocation.
        const decodedToken = await firebaseAuth.verifyIdToken(
            idToken,
            true
        );

        if (!decodedToken.uid) {

            return next(
                new Error("Invalid authentication identity")
            );
        }

        // Store verified identity on socket.
        socket.data.uid = decodedToken.uid;

        next();

    } catch (error) {

        console.warn(
            "[AUTH] Socket authentication rejected"
        );

        next(
            new Error("Firebase authentication failed")
        );
    }
});

// =====================================
// SOCKET.IO CONNECTION
// =====================================

io.on("connection", (socket) => {

    console.log(
        "[CONNECT] Authenticated client connected"
    );

    // =================================
    // JOIN ROOM
    // =================================

    socket.on("join-room", async (data) => {

        try {

            const roomId = String(
                data?.roomId || ""
            ).trim();

            // Client-supplied role is deliberately ignored.
            const uid = socket.data.uid;

            // Validate room code.
            if (!/^[1-9][0-9]{5}$/.test(roomId)) {

                socket.emit("join-error", {
                    message: "Invalid room code"
                });

                return;
            }

            if (!uid) {

                socket.emit("join-error", {
                    message: "Authentication required"
                });

                return;
            }

            // Read authoritative room information from Firebase.
            const snapshot = await firebaseDb
                .ref(`sessions/${roomId}`)
                .get();

            if (!snapshot.exists()) {

                socket.emit("join-error", {
                    message: "Room does not exist"
                });

                return;
            }

            const session = snapshot.val();

            // Room must already be connected in Firebase.
            if (
                !session ||
                session.status !== "connected" ||
                typeof session.senderUid !== "string" ||
                typeof session.receiverUid !== "string" ||
                !session.senderUid ||
                !session.receiverUid
            ) {

                socket.emit("join-error", {
                    message: "Room is not ready"
                });

                return;
            }

            // Determine role using verified Firebase UID.
            let verifiedRole = null;

            if (uid === session.senderUid) {

                verifiedRole = "sender";

            } else if (uid === session.receiverUid) {

                verifiedRole = "receiver";
            }

            if (!verifiedRole) {

                socket.emit("join-error", {
                    message: "You are not authorized to join this room"
                });

                return;
            }

            // Check duplicate role BEFORE leaving previous room.
            const members = await io
                .in(roomId)
                .fetchSockets();

            const duplicateRole = members.some((member) => {

                return member.id !== socket.id &&
                    member.data.role === verifiedRole;

            });

            if (duplicateRole) {

                socket.emit("join-error", {
                    message:
                        `A ${verifiedRole} is already connected to this room`
                });

                return;
            }

            // Leave previous room only after authorization succeeds.
            const previousRoom = socket.data.roomId;

            if (previousRoom && previousRoom !== roomId) {

                socket.to(previousRoom).emit("peer-status", {

                    status: "disconnected",
                    role: socket.data.role

                });

                await socket.leave(previousRoom);
            }

            // Join authorized room.
            await socket.join(roomId);

            socket.data.roomId = roomId;
            socket.data.role = verifiedRole;

            console.log(
                `[JOIN] Authorized ${verifiedRole.toUpperCase()} joined a room`
            );

            // Confirm joining.
            socket.emit("joined-room", {

                roomId: roomId,
                role: verifiedRole,
                message: "Successfully joined relay room"

            });

            // Notify other phone.
            socket.to(roomId).emit("peer-status", {

                status: "connected",
                role: verifiedRole

            });

            // Get room members.
            const roomMembers = await io
                .in(roomId)
                .fetchSockets();

            io.to(roomId).emit("room-status", {

                roomId: roomId,

                members: roomMembers.map((member) => ({
                    role: member.data.role
                }))

            });

            console.log(
                `[ROOM] Authorized members: ${roomMembers.length}`
            );

        } catch (error) {

            console.error(
                "[JOIN ERROR]",
                error.message
            );

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

            const message = String(
                data?.message || ""
            ).trim();

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

            // Forward message to other phone.
            socket.to(roomId).emit(
                "receive-message",
                messageData
            );

            // Confirm sender.
            socket.emit("message-sent", {

                status: "success",
                message: "Message forwarded to room"

            });

        } catch (error) {

            console.error(
                "[MESSAGE ERROR]",
                error.message
            );

            socket.emit("message-error", {
                message: "Server failed to send message"
            });
        }
    });

    // =================================
    // BINARY IP PACKET RELAY
    // =================================

    socket.on("ip-packet", (packet) => {

        try {

            const roomId = socket.data.roomId;
            const role = socket.data.role;

            if (!roomId || !role) {

                socket.emit("packet-error", {
                    message: "Join a room before sending packets"
                });

                return;
            }

            if (
                role !== "sender" &&
                role !== "receiver"
            ) {

                socket.emit("packet-error", {
                    message: "Invalid sender role"
                });

                return;
            }

            if (!Buffer.isBuffer(packet)) {

                socket.emit("packet-error", {
                    message: "Invalid packet format"
                });

                return;
            }

            if (
                packet.length < 20 ||
                packet.length > 1500
            ) {

                socket.emit("packet-error", {
                    message: "Invalid IP packet size"
                });

                return;
            }

            // Validate IPv4 packet.
            const version = packet[0] >> 4;

            const headerLength =
                (packet[0] & 0x0F) * 4;

            const totalLength =
                packet.readUInt16BE(2);

            if (
                version !== 4 ||
                headerLength < 20 ||
                headerLength > packet.length ||
                totalLength < headerLength ||
                totalLength !== packet.length
            ) {

                socket.emit("packet-error", {
                    message: "Invalid IPv4 packet"
                });

                return;
            }

            // Forward packet to opposite peer.
            socket.to(roomId).emit(
                "ip-packet",
                packet
            );

        } catch (error) {

            console.error(
                "[PACKET RELAY ERROR]",
                error.message
            );

            socket.emit("packet-error", {
                message: "Server failed to relay packet"
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
            "[DISCONNECTED]",
            reason
        );

        if (roomId) {

            socket.to(roomId).emit("peer-status", {

                status: "disconnected",
                role: role

            });

            try {

                const remainingMembers = await io
                    .in(roomId)
                    .fetchSockets();

                io.to(roomId).emit("room-status", {

                    roomId: roomId,

                    members: remainingMembers.map((member) => ({
                        role: member.data.role
                    }))

                });

            } catch (error) {

                console.error(
                    "[DISCONNECT ERROR]",
                    error.message
                );
            }
        }
    });

    // =================================
    // SOCKET ERROR
    // =================================

    socket.on("error", (error) => {

        console.error(
            "[SOCKET ERROR]",
            error.message
        );
    });

});

// =====================================
// ENGINE.IO CONNECTION ERRORS
// =====================================

io.engine.on("connection_error", (error) => {

    console.error("[ENGINE.IO ERROR]", {

        code: error.code,
        message: error.message

    });
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
        process.env.RENDER
            ? "RENDER CLOUD"
            : "LOCAL COMPUTER"
    );

    console.log(
        "Firebase: AUTHENTICATION ENABLED"
    );

    console.log("================================\n");
});

// =====================================
// SERVER ERROR HANDLING
// =====================================

server.on("error", (error) => {

    console.error(
        "[SERVER ERROR]",
        error.message
    );
});

// =====================================
// GRACEFUL SHUTDOWN
// =====================================

process.on("SIGTERM", () => {

    console.log(
        "SIGTERM received. Closing server..."
    );

    io.close(() => {

        server.close(() => {

            console.log(
                "RemoteNet server stopped."
            );

            process.exit(0);
        });
    });
});
