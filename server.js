import { WebSocketServer } from "ws";
import { createServer } from "node:http";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 8080;
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "contacts.json");

// --- Простое хранилище списков контактов (кто кому разрешил писать) ---
// { username: ["friend1", "friend2", ...] }
let contacts = {};

async function loadContacts() {
  try {
    if (!existsSync(DATA_FILE)) return;
    contacts = JSON.parse(await readFile(DATA_FILE, "utf8"));
  } catch (e) {
    console.error("Не удалось прочитать контакты:", e.message);
    contacts = {};
  }
}

async function saveContacts() {
  try {
    await mkdir(DATA_DIR, { recursive: true });
    await writeFile(DATA_FILE, JSON.stringify(contacts, null, 2), "utf8");
  } catch (e) {
    console.error("Не удалось сохранить контакты:", e.message);
  }
}

// --- Состояние подключённых абонентов ---
// username -> { ws, isOnline }
const clients = new Map();

function broadcastContacts() {
  const list = [...clients.keys()];
  const msg = JSON.stringify({ type: "online_list", online: list });
  for (const c of clients.values()) c.ws.send(msg);
}

function send(socket, obj) {
  if (socket.readyState === 1) socket.send(JSON.stringify(obj));
}

// --- Сервер ---
const httpServer = createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("P2P relay is running");
});

const wss = new WebSocketServer({
  server: httpServer,
  perMessageDeflate: false,
});

httpServer.listen(PORT, () => {
  console.log(`Ретранслятор слушает порт ${PORT}`);
});

wss.on("connection", (ws) => {
  let username = null;

  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", code: "BAD_MSG", text: "Невалидный формат" });
    }

    switch (msg.type) {
      // Логин: сообщить имя. Простоим, без паролей по требованию пользователя.
      case "login": {
        const name = String(msg.username || "").trim();
        if (!name) return send(ws, { type: "error", code: "NO_NAME", text: "Имя не задано" });

        // Если уже кто-то online с таким именем — отключаем старого (второй вход).
        if (clients.has(name)) {
          const prev = clients.get(name);
          prev.isOnline = false;
          try { prev.ws.close(4001, "Замена соединения"); } catch {}
          clients.delete(name);
        }

        username = name;
        contacts[username] = contacts[username] || [];
        clients.set(username, { ws, isOnline: true });
        await saveContacts();
        send(ws, { type: "login_ok", username, contacts: contacts[username] });
        broadcastContacts();
        return;
      }

      // Добавить в свой список разрешённых контактов.
      case "add_contact": {
        if (!username) return send(ws, { type: "error", code: "NOT_LOGGED", text: "Сначала войдите" });
        const contact = String(msg.contact || "").trim();
        if (!contact || contact === username)
          return send(ws, { type: "error", code: "BAD_CONTACT", text: "Некорректный контакт" });
        if (!contacts[username].includes(contact)) contacts[username].push(contact);
        await saveContacts();
        send(ws, { type: "contacts_updated", contacts: contacts[username] });
        return;
      }

      // Переслать сообщение адресату, если он онлайн И в нашем списке контактов.
      case "message": {
        if (!username) return send(ws, { type: "error", code: "NOT_LOGGED", text: "Сначала войдите" });
        const to = String(msg.to || "").trim();
        const body = String(msg.body ?? "").slice(0, 4000);

        if (!contacts[username].includes(to)) {
          return send(ws, {
            type: "error", code: "NOT_CONTACT",
            text: `${to} нет в вашем списке контактов`,
          });
        }

        const target = clients.get(to);
        if (!target || !target.isOnline) {
          return send(ws, { type: "message_status", to, status: "offline" });
        }

        target.ws.send(JSON.stringify({
          type: "message",
          from: username,
          body,
        }));
        send(ws, { type: "message_status", to, status: "delivered" });
        return;
      }

      default:
        return send(ws, { type: "error", code: "UNKNOWN", text: "Неизвестный тип запроса" });
    }
  });

  ws.on("close", () => {
    if (username && clients.get(username)?.ws === ws) {
      clients.delete(username);
      broadcastContacts();
    }
  });

  ws.on("error", () => {});
});

await loadContacts();