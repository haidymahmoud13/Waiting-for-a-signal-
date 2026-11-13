import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { GoogleGenAI, ThinkingLevel } from '@google/genai';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = 3000;
const IS_PROD = process.env.NODE_ENV === 'production';

// --- Types (erasable TS syntax for Node native strip-types + tsx) ---
type MissionId = 'apollo11' | 'apollo15' | 'ingenuity';
type Level2Mode = 'global_sim' | 'room' | 'solo_ai';

interface ChatMessage {
  id: string;
  senderName: string;
  senderRole?: string;
  senderCountry?: string;
  kind: 'player' | 'ai_teammate' | 'remote_player' | 'system';
  text: string;
  timestamp: number;
}

interface RoomMember {
  clientId: string;
  name: string;
  country?: string;
  roleId: string | null;
  isAi: boolean;
  completedTaskIds: string[];
}

interface RoomState {
  code: string;
  missionId: MissionId;
  mode: Level2Mode;
  createdAt: number;
  members: RoomMember[];
  completedTaskIds: string[];
  messages: ChatMessage[];
  missionComplete: boolean;
}

interface CachedNasaMedia {
  nasaId: string;
  title: string;
  description: string;
  imageUrl: string | null;
  detailsUrl: string;
  dateCreated?: string;
  center?: string;
  isFallback: boolean;
}

// --- Gemini Client ---
function getGeminiClient(): GoogleGenAI | null {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) return null;
  return new GoogleGenAI({
    apiKey,
    httpOptions: {
      headers: {
        'User-Agent': 'aistudio-build',
      },
    },
  });
}

// --- NASA Media Cache ---
const nasaMediaCache = new Map<string, CachedNasaMedia>();

async function fetchNasaImageSearch(query: string, fallbackId?: string): Promise<CachedNasaMedia> {
  const cacheKey = `${query}::${fallbackId || ''}`;
  const cached = nasaMediaCache.get(cacheKey);
  if (cached) return cached;

  const tryFetch = async (url: string): Promise<CachedNasaMedia | null> => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    try {
      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok) return null;
      const json = (await res.json()) as {
        collection?: {
          items?: Array<{
            data?: Array<{
              nasa_id?: string;
              title?: string;
              description?: string;
              date_created?: string;
              center?: string;
            }>;
            links?: Array<{ href?: string; rel?: string; render?: string }>;
          }>;
        };
      };
      const items = json?.collection?.items || [];
      for (const item of items) {
        const meta = item.data?.[0];
        const imgLink = item.links?.find((l) => l.href && (l.render === 'image' || l.rel === 'preview'));
        if (meta?.nasa_id && imgLink?.href) {
          return {
            nasaId: meta.nasa_id,
            title: meta.title || query,
            description: (meta.description || '').slice(0, 260),
            imageUrl: imgLink.href.replace(/^http:\/\//i, 'https://'),
            detailsUrl: `https://images.nasa.gov/details/${encodeURIComponent(meta.nasa_id)}`,
            dateCreated: meta.date_created,
            center: meta.center,
            isFallback: false,
          };
        }
      }
      return null;
    } catch {
      return null;
    } finally {
      clearTimeout(timeout);
    }
  };

  let result: CachedNasaMedia | null = null;
  if (fallbackId) {
    result = await tryFetch(
      `https://images-api.nasa.gov/search?nasa_id=${encodeURIComponent(fallbackId)}&media_type=image`
    );
  }
  if (!result) {
    result = await tryFetch(
      `https://images-api.nasa.gov/search?q=${encodeURIComponent(query)}&media_type=image&page_size=6`
    );
  }
  if (!result) {
    result = {
      nasaId: fallbackId || 'NASA-ARCHIVE',
      title: query,
      description: 'Official NASA image preview could not be reached right now. See NASA Image Library link for verified archival photography.',
      imageUrl: null,
      detailsUrl: fallbackId
        ? `https://images.nasa.gov/details/${encodeURIComponent(fallbackId)}`
        : `https://images.nasa.gov/search?q=${encodeURIComponent(query)}`,
      isFallback: true,
    };
  }
  nasaMediaCache.set(cacheKey, result);
  return result;
}

// --- Mission Roles Registry for Rooms ---
const MISSION_ROLES: Record<MissionId, Array<{ id: string; name: string; title: string }>> = {
  apollo11: [
    { id: 'armstrong', name: 'Neil Armstrong', title: 'Commander (Eagle)' },
    { id: 'aldrin', name: 'Buzz Aldrin', title: 'Lunar Module Pilot' },
    { id: 'collins', name: 'Michael Collins', title: 'Command Module Pilot (Columbia)' },
  ],
  apollo15: [
    { id: 'scott', name: 'David Scott', title: 'Commander (Falcon / LRV)' },
    { id: 'irwin', name: 'James Irwin', title: 'Lunar Module Pilot (ALSEP)' },
    { id: 'worden', name: 'Alfred Worden', title: 'Command Module Pilot (Endeavour)' },
  ],
  ingenuity: [
    { id: 'navigation_analyst', name: 'Navigation Analyst', title: 'Fictional Game Role' },
    { id: 'flight_dynamics_analyst', name: 'Flight Dynamics Analyst', title: 'Fictional Game Role' },
    { id: 'landing_systems_analyst', name: 'Landing Systems Analyst', title: 'Fictional Game Role' },
  ],
};

const MISSION_TASK_TOTALS: Record<MissionId, number> = {
  apollo11: 3,
  apollo15: 3,
  ingenuity: 3,
};

// --- Authoritative Multiplayer Rooms (Permanent Session Codes + Disk Persistence) ---
const ROOMS_STORE_PATH = path.join(os.tmpdir(), 'waiting-for-a-signal-rooms.json');
const rooms = new Map<string, RoomState>();
const roomSockets = new Map<string, Set<WebSocket>>();
const socketMeta = new Map<WebSocket, { roomCode: string | null; clientId: string }>();

function normalizeRoomCode(raw: string): string {
  return raw.toUpperCase().replace(/[^A-Z0-9]/g, '').trim();
}

function saveRoomsToDisk(): void {
  try {
    const serializable: RoomState[] = [];
    for (const room of rooms.values()) {
      serializable.push(room);
    }
    fs.writeFileSync(ROOMS_STORE_PATH, JSON.stringify(serializable), 'utf8');
  } catch {
    // Non-fatal if temp directory is read-only
  }
}

function loadRoomsFromDisk(): void {
  try {
    if (!fs.existsSync(ROOMS_STORE_PATH)) return;
    const raw = fs.readFileSync(ROOMS_STORE_PATH, 'utf8');
    const parsed = JSON.parse(raw) as RoomState[];
    if (!Array.isArray(parsed)) return;
    for (const r of parsed) {
      if (r && r.code) {
        const clean = normalizeRoomCode(r.code);
        if (!clean) continue;
        rooms.set(clean, {
          code: clean,
          missionId: r.missionId || 'apollo11',
          mode: r.mode || 'room',
          createdAt: r.createdAt || Date.now(),
          members: Array.isArray(r.members) ? r.members : [],
          completedTaskIds: Array.isArray(r.completedTaskIds) ? r.completedTaskIds : [],
          messages: Array.isArray(r.messages) ? r.messages : [],
          missionComplete: Boolean(r.missionComplete),
        });
        roomSockets.set(clean, new Set());
      }
    }
  } catch {
    // Ignore malformed store
  }
}

loadRoomsFromDisk();

function generateRoomCode(): string {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return rooms.has(code) ? generateRoomCode() : code;
}

function broadcastRoom(code: string): void {
  const room = rooms.get(code);
  const sockets = roomSockets.get(code);
  if (!room || !sockets) return;
  const payload = JSON.stringify({ type: 'room:state', room });
  for (const ws of sockets) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  }
}

function createRoom(missionId: MissionId, mode: Level2Mode, customCode?: string): RoomState {
  const cleanCustom = customCode ? normalizeRoomCode(customCode).slice(0, 10) : '';
  if (cleanCustom && rooms.has(cleanCustom)) {
    return rooms.get(cleanCustom)!;
  }
  const code = cleanCustom || generateRoomCode();
  const now = Date.now();
  const room: RoomState = {
    code,
    missionId,
    mode,
    createdAt: now,
    members: [],
    completedTaskIds: [],
    messages: [
      {
        id: `sys-init-${now}`,
        senderName: 'Mission Control',
        kind: 'system',
        text:
          mode === 'room'
            ? `Team "${code}" created! Share this Team Code anytime so classmates or friends can join together.`
            : mode === 'global_sim'
              ? `Universal Team room ${code} opened for ${missionId.toUpperCase()}. Players from different countries match here to collaborate.`
              : `Individual AI Team initialized for ${missionId.toUpperCase()}. AI teammates are ready to assist you.`,
        timestamp: now,
      },
    ],
    missionComplete: false,
  };
  rooms.set(code, room);
  roomSockets.set(code, new Set());
  saveRoomsToDisk();
  return room;
}

function assignRoleForMember(room: RoomState, member: RoomMember, preferredRoleId?: string | null): boolean {
  const roles = MISSION_ROLES[room.missionId] || [];
  const isValidPreferred = Boolean(preferredRoleId && roles.some((r) => r.id === preferredRoleId));
  const preferredTakenByHuman = Boolean(
    isValidPreferred &&
      room.members.some((m) => m.roleId === preferredRoleId && m.clientId !== member.clientId && !m.isAi)
  );

  if (isValidPreferred && !preferredTakenByHuman && preferredRoleId) {
    room.members = room.members.filter((m) => !(m.isAi && m.roleId === preferredRoleId));
    member.roleId = preferredRoleId;
    return true;
  }

  const openRole = roles.find(
    (r) => !room.members.some((m) => m.roleId === r.id && m.clientId !== member.clientId && !m.isAi)
  );
  if (openRole) {
    room.members = room.members.filter((m) => !(m.isAi && m.roleId === openRole.id));
    member.roleId = openRole.id;
    return true;
  }
  return false;
}

function joinMemberToRoom(
  room: RoomState,
  clientId: string,
  playerNameRaw?: string,
  countryRaw?: string,
  preferredRoleId?: string | null
): { ok: true } | { ok: false; error: string } {
  const existing = room.members.find((m) => m.clientId === clientId);
  if (existing) {
    if (!existing.roleId) {
      assignRoleForMember(room, existing, preferredRoleId);
    }
    return { ok: true };
  }

  const humanCount = room.members.filter((m) => !m.isAi).length;
  if (humanCount >= 3) {
    return {
      ok: false,
      error: `Team "${room.code}" already has all 3 human players connected. Create a new Team Code or join another open team!`,
    };
  }

  const playerName = (playerNameRaw || 'Explorer').trim().slice(0, 24) || 'Explorer';
  const country = (countryRaw || 'International').trim().slice(0, 32) || 'International';
  const member: RoomMember = {
    clientId,
    name: playerName,
    country,
    roleId: null,
    isAi: false,
    completedTaskIds: [],
  };
  room.members.push(member);
  assignRoleForMember(room, member, preferredRoleId);

  const newHumanCount = room.members.filter((m) => !m.isAi).length;
  room.messages.push({
    id: `join-${clientId}-${Date.now()}`,
    senderName: 'Mission Control',
    kind: 'system',
    text:
      room.mode === 'global_sim'
        ? `${playerName} from ${country} connected to Universal Team!`
        : `${playerName} joined team "${room.code}".`,
    timestamp: Date.now(),
  });

  if (room.mode === 'global_sim' && newHumanCount === 1) {
    room.messages.push({
      id: `alone-${Date.now()}`,
      senderName: 'Mission Control',
      kind: 'system',
      text: 'No other players are playing in Universal Team right now. You can switch to Form / Join a Team or Play Individually with AI Bots, or wait here for a teammate.',
      timestamp: Date.now(),
    });
  } else if (room.mode === 'global_sim' && newHumanCount >= 2) {
    room.messages.push({
      id: `collab-${Date.now()}`,
      senderName: 'Mission Control',
      kind: 'system',
      text: `Collaborative Universal Team active (${newHumanCount} players connected)! Coordinate together in Team Chat.`,
      timestamp: Date.now(),
    });
  }

  if (room.messages.length > 60) {
    room.messages = room.messages.slice(-60);
  }
  saveRoomsToDisk();
  broadcastRoom(room.code);
  return { ok: true };
}

async function generateTeamBotReply(room: RoomState, question: string, senderName: string): Promise<string> {
  const ai = getGeminiClient();
  const missionFacts: Record<MissionId, string> = {
    apollo11:
      'Apollo 11 Level 2 Team Sequence: 1) Neil Armstrong and Buzz Aldrin descend to Tranquility Base and finish lunar surface operations. 2) Eagle’s ascent stage lifts off from the Moon into lunar orbit. 3) Michael Collins tracks Eagle aboard Columbia for orbital rendezvous. 4) Eagle and Columbia dock in lunar orbit and transfer the crew and lunar samples. 5) Command Module Columbia returns all three astronauts safely to Earth on July 24, 1969.',
    apollo15:
      'Apollo 15 Level 2 Team Sequence: 1) David Scott and James Irwin explore Hadley-Apennine using the Lunar Roving Vehicle (~17.5 miles) and collect >170 lb of samples. 2) Alfred Worden operates orbital instruments aboard Endeavour. 3) Falcon lifts off from the lunar surface and docks with Endeavour in lunar orbit. 4) On the return journey after docking, Worden performs a deep-space EVA to retrieve SIM-bay film cassettes. 5) The crew returns safely to Earth on August 7, 1971.',
    ingenuity:
      'Ingenuity Flight 6 Team Sequence: 1) Ingenuity’s camera system loses one navigation-camera image during Flight 6. 2) Subsequent images receive inaccurate timestamps. 3) Inconsistent navigation estimates cause the helicopter to oscillate, though it remains controllable. 4) Navigation-camera images are not used during the final landing phase. 5) Ingenuity touches down safely about five metres from its intended location.',
  };

  if (ai) {
    try {
      const response = await ai.models.generateContent({
        model: 'gemini-4-hydrogen',
        contents: `Player ${senderName} asks inside Level 2 Team Chat: "${question}"`,
        config: {
          systemInstruction: [
            'You are SIGNAL Bot inside the Level 2 multiplayer team chat of "Waiting for a Signal".',
            'Give helpful, concise (1-3 sentences) team hints and mission guidance without spoiling everything at once. Reply in the same language as the player (Arabic or English).',
            `Current Mission: ${room.missionId}. Completed tasks so far: ${room.completedTaskIds.join(', ') || 'none'}.`,
            missionFacts[room.missionId],
          ].join('\n'),
          thinkingConfig: { thinkingLevel: ThinkingLevel.LOW },
        },
      });
      const text = (response.text || '').trim();
      if (text) return text;
    } catch {
      // fall through to deterministic mission reply
    }
  }

  return `Team Hint (${room.missionId.toUpperCase()}): Check each astronaut/role task status until all 3 show Complete, then order the 5 real mission steps below (skipping the 2 distractors).`;
}

async function startServer(): Promise<void> {
  const app = express();
  app.use(express.json({ limit: '1mb' }));

  // Serve public assets (including uploaded PDFs in /public/assets/aistudio)
  app.use('/assets', express.static(path.join(__dirname, 'public', 'assets')));

  // Health check
  app.get('/api/health', (_req, res) => {
    res.json({ ok: true, service: 'waiting-for-a-signal' });
  });

  // NASA verified media proxy
  app.get('/api/nasa/media', async (req, res) => {
    const q = String(req.query.q || '').trim();
    const fallbackId = req.query.fallbackId ? String(req.query.fallbackId).trim() : undefined;
    if (!q) {
      res.status(400).json({ error: 'Missing search query q' });
      return;
    }
    const media = await fetchNasaImageSearch(q, fallbackId);
    res.json(media);
  });

  // List open teams for smooth 1-click joining
  app.get('/api/rooms', (req, res) => {
    const filterMission = req.query.missionId ? String(req.query.missionId) : '';
    const openTeams = Array.from(rooms.values())
      .filter((r) => {
        if (r.mode !== 'room' || r.missionComplete) return false;
        if (filterMission && r.missionId !== filterMission) return false;
        return r.members.filter((m) => !m.isAi).length < 3;
      })
      .sort((a, b) => b.createdAt - a.createdAt)
      .slice(0, 12)
      .map((r) => ({
        code: r.code,
        missionId: r.missionId,
        humanCount: r.members.filter((m) => !m.isAi).length,
        memberNames: r.members.filter((m) => !m.isAi).map((m) => m.name),
        createdAt: r.createdAt,
      }));
    res.json({ teams: openTeams });
  });

  // Create (or join by customCode) a room via HTTP
  app.post('/api/rooms', (req, res) => {
    const missionId = (req.body?.missionId as MissionId) || 'apollo11';
    const mode = (req.body?.mode as Level2Mode) || 'room';
    const customCode = typeof req.body?.customCode === 'string' ? req.body.customCode : undefined;
    if (!MISSION_ROLES[missionId]) {
      res.status(400).json({ error: 'Invalid missionId' });
      return;
    }
    const cleanCustom = customCode ? normalizeRoomCode(customCode).slice(0, 10) : '';
    const existed = Boolean(cleanCustom && rooms.has(cleanCustom));
    const room = createRoom(missionId, mode, cleanCustom || undefined);
    res.json({ room, joinedExisting: existed });
  });

  // Universal Matchmaking: Match players across regions into an active human global_sim room for that mission
  app.post('/api/rooms/universal', (req, res) => {
    const missionId = (req.body?.missionId as MissionId) || 'apollo11';
    if (!MISSION_ROLES[missionId]) {
      res.status(400).json({ error: 'Invalid missionId' });
      return;
    }

    const candidates = Array.from(rooms.values()).filter((candidate) => {
      const humanCount = candidate.members.filter((m) => !m.isAi).length;
      const hasAiBackup = candidate.members.some((m) => m.isAi);
      const recentlyOpened = Date.now() - candidate.createdAt < 15000 && candidate.completedTaskIds.length === 0;
      return (
        candidate.mode === 'global_sim' &&
        candidate.missionId === missionId &&
        !candidate.missionComplete &&
        !hasAiBackup &&
        humanCount < 3 &&
        (humanCount >= 1 || recentlyOpened)
      );
    });

    candidates.sort((a, b) => {
      const aHumans = a.members.filter((m) => !m.isAi).length;
      const bHumans = b.members.filter((m) => !m.isAi).length;
      return bHumans - aHumans;
    });

    const matched = candidates[0];
    const room = matched || createRoom(missionId, 'global_sim');
    res.json({ room, joinedExisting: Boolean(matched) });
  });

  app.get('/api/rooms/:code', (req, res) => {
    const code = normalizeRoomCode(String(req.params.code || ''));
    if (!code) {
      res.status(400).json({ error: 'Please enter a valid Team Code.' });
      return;
    }
    const existing = rooms.get(code);
    if (!existing) {
      res.status(404).json({
        error: `No team found with code "${code}" yet. Check the spelling or create a team with that code!`,
      });
      return;
    }
    res.json({ room: existing });
  });

  // HTTP Join fallback (works even if WebSockets are blocked or slow)
  app.post('/api/rooms/:code/join', (req, res) => {
    const code = normalizeRoomCode(String(req.params.code || ''));
    const room = rooms.get(code);
    if (!room) {
      res.status(404).json({
        error: `No team found with code "${code}" yet.`,
      });
      return;
    }
    const clientId =
      typeof req.body?.clientId === 'string' && req.body.clientId.trim()
        ? req.body.clientId.trim().slice(0, 32)
        : `plr-${Math.random().toString(36).slice(2, 9)}`;
    const result = joinMemberToRoom(
      room,
      clientId,
      req.body?.playerName,
      req.body?.country,
      req.body?.roleId
    );
    if (!result.ok) {
      res.status(409).json({ error: result.error });
      return;
    }
    res.json({ clientId, room });
  });

  // HTTP Action endpoint (works alongside WebSockets for role claims, completions, chat, and AI bot)
  app.post('/api/rooms/:code/action', (req, res) => {
    const code = normalizeRoomCode(String(req.params.code || ''));
    const room = rooms.get(code);
    if (!room) {
      res.status(404).json({ error: `Team "${code}" was not found.` });
      return;
    }
    const { type, clientId, roleId, taskId, text, messageId } = req.body || {};
    if (type === 'leave' && clientId) {
      const departing = room.members.find((m) => m.clientId === clientId);
      room.members = room.members.filter((m) => m.clientId !== clientId);
      if (departing) {
        room.messages.push({
          id: `leave-${clientId}-${Date.now()}`,
          senderName: 'Mission Control',
          kind: 'system',
          text: `${departing.name} left the team.`,
          timestamp: Date.now(),
        });
        if (
          room.mode === 'global_sim' &&
          room.members.filter((m) => !m.isAi).length === 0 &&
          room.completedTaskIds.length === 0
        ) {
          rooms.delete(code);
          roomSockets.delete(code);
        }
        saveRoomsToDisk();
        broadcastRoom(code);
      }
      res.json({ room });
      return;
    }

    if (type === 'role:claim' && clientId && roleId) {
      const member = room.members.find((m) => m.clientId === clientId);
      if (member) {
        const claimed = assignRoleForMember(room, member, roleId);
        if (!claimed) {
          res.status(409).json({ error: 'All 3 mission roles are already claimed.' });
          return;
        }
        saveRoomsToDisk();
        broadcastRoom(code);
      }
      res.json({ room });
      return;
    }

    if (type === 'room:fill_ai') {
      const roles = MISSION_ROLES[room.missionId] || [];
      const simNames =
        room.mode === 'global_sim'
          ? ['DSN Canberra (Sim)', 'DSN Madrid (Sim)', 'Houston Sim']
          : ['SIGNAL-Bot Alpha', 'SIGNAL-Bot Beta', 'SIGNAL-Bot Gamma'];
      let idx = 0;
      for (const r of roles) {
        if (!room.members.some((m) => m.roleId === r.id)) {
          room.members.push({
            clientId: `ai-${r.id}`,
            name: `${simNames[idx % simNames.length]} · ${r.name}`,
            roleId: r.id,
            isAi: true,
            completedTaskIds: [],
          });
          idx++;
        }
      }
      saveRoomsToDisk();
      broadcastRoom(code);
      res.json({ room });
      return;
    }

    if (type === 'task:complete' && taskId) {
      if (!room.completedTaskIds.includes(taskId)) {
        room.completedTaskIds.push(taskId);
        const actor = room.members.find(
          (m) => m.clientId === clientId || (roleId && m.roleId === roleId)
        );
        if (actor && !actor.completedTaskIds.includes(taskId)) {
          actor.completedTaskIds.push(taskId);
        }
        if (taskId.startsWith('role:') && actor && !actor.isAi) {
          const rId = taskId.replace('role:', '');
          const roleMeta = MISSION_ROLES[room.missionId]?.find((r) => r.id === rId);
          room.messages.push({
            id: `done-${clientId || 'p'}-${rId}-${Date.now()}`,
            senderName: 'Mission Control',
            kind: 'system',
            text: `${actor.name}${actor.country ? ` (${actor.country})` : ''} completed ${roleMeta?.name || rId}’s station task!`,
            timestamp: Date.now(),
          });
        }
        const completedRolesCount = room.completedTaskIds.filter((t) => t.startsWith('role:')).length;
        const totalNeeded = MISSION_TASK_TOTALS[room.missionId] || 3;
        if (taskId === 'shared:sequence' && completedRolesCount >= totalNeeded) {
          room.missionComplete = true;
          room.messages.push({
            id: `win-${Date.now()}`,
            senderName: 'Mission Control',
            kind: 'system',
            text: 'All 3 team roles and the shared mission sequence are verified! Mission complete.',
            timestamp: Date.now(),
          });
        }
        saveRoomsToDisk();
        broadcastRoom(code);
      }
      res.json({ room });
      return;
    }

    if ((type === 'chat:send' || type === 'ai:ask') && typeof text === 'string') {
      const cleanText = text.trim().slice(0, 320);
      if (!cleanText) {
        res.json({ room });
        return;
      }
      const msgId = messageId || `${type === 'ai:ask' ? 'ask' : 'chat'}-${clientId || 'p'}-${Date.now()}`;
      if (!room.messages.some((m) => m.id === msgId)) {
        const member = room.members.find((m) => m.clientId === clientId);
        const roleMeta = MISSION_ROLES[room.missionId]?.find((r) => r.id === member?.roleId);
        room.messages.push({
          id: msgId,
          senderName: member?.name || 'Explorer',
          senderRole: roleMeta?.name,
          senderCountry: member?.country,
          kind: 'player',
          text: cleanText,
          timestamp: Date.now(),
        });
        if (room.messages.length > 60) room.messages = room.messages.slice(-60);
        saveRoomsToDisk();
        broadcastRoom(code);

        const asksBot =
          type === 'ai:ask' ||
          /@(ai|signal|bot)\b/i.test(cleanText) ||
          (room.mode === 'solo_ai' && /\?/.test(cleanText));
        if (asksBot) {
          void generateTeamBotReply(room, cleanText, member?.name || 'Explorer').then((reply) => {
            const current = rooms.get(code);
            if (!current) return;
            current.messages.push({
              id: `ai-bot-${Date.now()}`,
              senderName: 'SIGNAL AI Bot',
              kind: 'ai_teammate',
              text: reply,
              timestamp: Date.now(),
            });
            saveRoomsToDisk();
            broadcastRoom(code);
          });
        }
      }
      res.json({ room });
      return;
    }

    res.json({ room });
  });

  // Persistent Gemini AI Helper endpoint
  app.post('/api/ai/helper', async (req, res) => {
    const ai = getGeminiClient();
    const {
      missionName = 'NASA Mission',
      roleName = 'Mission Specialist',
      level = 1,
      objective = '',
      stepTitle = '',
      stepInstruction = '',
      cluesContext = '',
      question = '',
      history = [],
    } = req.body || {};

    if (!question || typeof question !== 'string') {
      res.status(400).json({ error: 'Please enter a question for the AI helper.' });
      return;
    }

    if (!ai) {
      res.status(503).json({
        error: 'Gemini AI helper is currently unavailable because GEMINI_API_KEY is not configured in Secrets.',
      });
      return;
    }

    const knowledgeBase = [
      '=== GAME KNOWLEDGE BASE: WAITING FOR A SIGNAL ===',
      'GAME STRUCTURE: 3 machines (Apollo 11, Apollo 15, Ingenuity Mars Helicopter). Each machine has 2 levels:',
      '- Level 1 (Individual Challenge): Player chooses ONE role and plays ONLY that role’s challenge: situation + 2 Help Pop-ups + 5-step ordered connection puzzle (plus 2 extra distractor choices) + Check Answer + result card.',
      '- Level 2 (Team Challenge): Player keeps the SAME role from Level 1, plays with AI Teammates (simulated), Simulated Team Mode, or a Live Room Code, completes their own role task while teammates complete the other two, then solves the 5-step shared mission puzzle.',
      'APOLLO 11 FACTS & ROLES (Real Crew):',
      '- Crew: Neil Armstrong (Commander), Buzz Aldrin (Lunar Module Pilot), Michael Collins (Command Module Pilot).',
      '- Key Facts: First crewed Moon landing (July 1969). Armstrong & Aldrin descended in Eagle while Collins stayed in lunar orbit aboard Columbia. 1202 alarms meant the guidance computer was overloaded with queued tasks, yet high-priority tasks ran; Steve Bales & Mission Control confirmed landing could continue. Near West Crater rough terrain, Armstrong took manual control to land on a flatter spot. After surface work, Eagle lifted off and rendezvoused/docked with Columbia so all 3 returned to Earth.',
      'APOLLO 15 FACTS & ROLES (Real Crew):',
      '- Crew: David Scott (Commander), James Irwin (Lunar Module Pilot), Alfred Worden (Command Module Pilot).',
      '- Key Facts: Launched July 26, 1971; landed Hadley-Apennine July 30, 1971; returned Aug 7, 1971. First mission with Lunar Roving Vehicle (~17.5 miles). Scott & Irwin collected >170 lb of documented geological samples. Worden operated orbital instruments in Endeavour, Falcon docked with Endeavour, and Worden retrieved SIM-bay film cassettes during a deep-space EVA on the return journey AFTER docking.',
      'INGENUITY MARS HELICOPTER FACTS & ROLES (Uncrewed Robotic Helicopter — Fictional Game Roles):',
      '- IMPORTANT: Ingenuity was an uncrewed robotic helicopter carried by Perseverance—NEVER claim an astronaut or human crew was onboard Ingenuity.',
      '- Fictional Game Roles: Navigation Analyst, Flight Dynamics Analyst, Landing Systems Analyst.',
      '- Key Facts: First powered controlled flight on Mars on April 19, 2021. Flight 6 took place May 22, 2021. One navigation-camera image was lost and later images received inaccurate timestamps, causing inconsistent navigation estimates and oscillating motion—yet Ingenuity remained controllable. Navigation-camera images were NOT used during final descent, so Ingenuity landed safely ~5 metres from its intended location.',
    ].join('\n');

    const systemInstruction = [
      'You are SIGNAL, the friendly AI Assistant inside the educational space game "Waiting for a Signal".',
      'Rules:',
      '1. Help students understand game controls (Back, Exit, Mission Facts, Help Pop-up 1 & 2, 5-step Ordered Connection Puzzle with 2 distractors, Level 2 Team modes) and real mission history.',
      '2. Keep historical facts strictly separate from fictional game mechanics. Never invent mission events or personnel. For Ingenuity, clearly remember the 3 roles are fictional game roles because Ingenuity was robotic.',
      '3. When helping with a puzzle, give clear helpful clues without just dumping the exact 5-step answer list immediately.',
      '4. Reply in clear, friendly, simple language (2 to 4 short sentences) suited for school students. Answer in Arabic if the user writes in Arabic, or English if they write in English.',
      knowledgeBase,
      `CURRENT PLAYER CONTEXT -> Mission: ${missionName} | Role: ${roleName} | Level: ${level} | Stage: ${stepTitle} | Instruction/Situation: ${stepInstruction || objective}${cluesContext ? ` | Available Help Pop-ups: ${cluesContext}` : ''}`,
    ].join('\n');

    const recentHistory = Array.isArray(history)
      ? history
          .slice(-6)
          .map((m: { role?: string; text?: string }) => `${m.role === 'user' ? 'Player' : 'SIGNAL'}: ${m.text || ''}`)
          .join('\n')
      : '';

    const prompt = recentHistory
      ? `Recent conversation:\n${recentHistory}\n\nPlayer now asks: ${question}`
      : `Player asks: ${question}`;

    try {
      const response = await ai.models.generateContent({
        model: 'gemini-4-hydrogen',
        contents: prompt,
        config: {
          systemInstruction,
          thinkingConfig: { thinkingLevel: ThinkingLevel.LOW },
        },
      });
      const reply = (response.text || '').trim();
      if (!reply) {
        res.status(502).json({ error: 'SIGNAL received an empty response. Please try asking again.' });
        return;
      }
      res.json({ reply });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      res.status(500).json({ error: `SIGNAL communication error: ${msg}` });
    }
  });

  // Vite dev middleware or production static assets
  if (!IS_PROD) {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws) => {
    const clientId = `plr-${Math.random().toString(36).slice(2, 9)}`;
    socketMeta.set(ws, { roomCode: null, clientId });

    ws.on('message', (raw) => {
      let msg: {
        type?: string;
        code?: string;
        missionId?: MissionId;
        mode?: Level2Mode;
        playerName?: string;
        country?: string;
        roleId?: string;
        taskId?: string;
        messageId?: string;
        text?: string;
      };
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }

      const meta = socketMeta.get(ws);
      if (!meta) return;

      if (msg.type === 'room:join' && msg.code) {
        const code = normalizeRoomCode(msg.code);
        const room = rooms.get(code);
        if (!room) {
          ws.send(
            JSON.stringify({
              type: 'room:error',
              error: `No team found with code "${code}" yet. Double-check the code or create a new team!`,
            })
          );
          return;
        }
        // Leave previous room if any
        if (meta.roomCode && meta.roomCode !== code) {
          roomSockets.get(meta.roomCode)?.delete(ws);
        }
        meta.roomCode = code;
        if (!roomSockets.has(code)) roomSockets.set(code, new Set());
        roomSockets.get(code)!.add(ws);

        // Allow client to reuse HTTP clientId or WS clientId cleanly
        const effectiveClientId =
          typeof (msg as { clientId?: string }).clientId === 'string' &&
          (msg as { clientId?: string }).clientId!.trim()
            ? (msg as { clientId?: string }).clientId!.trim().slice(0, 32)
            : meta.clientId;
        meta.clientId = effectiveClientId;

        const joinRes = joinMemberToRoom(
          room,
          effectiveClientId,
          msg.playerName,
          msg.country,
          msg.roleId
        );
        if (!joinRes.ok) {
          ws.send(JSON.stringify({ type: 'room:error', error: joinRes.error }));
          return;
        }
        ws.send(JSON.stringify({ type: 'room:joined', clientId: effectiveClientId, room }));
        broadcastRoom(code);
        return;
      }

      const code = meta.roomCode;
      if (!code) return;
      const room = rooms.get(code);
      if (!room) return;

      if (msg.type === 'role:claim' && msg.roleId) {
        const member = room.members.find((m) => m.clientId === meta.clientId);
        if (!member) return;
        const ok = assignRoleForMember(room, member, msg.roleId);
        if (!ok) {
          ws.send(
            JSON.stringify({
              type: 'room:error',
              error: 'All 3 mission roles in this live room are already claimed by connected players.',
            })
          );
          return;
        }
        saveRoomsToDisk();
        broadcastRoom(code);
        return;
      }

      if (msg.type === 'room:fill_ai') {
        const roles = MISSION_ROLES[room.missionId] || [];
        const simNames =
          room.mode === 'global_sim'
            ? ['DSN Canberra (Sim)', 'DSN Madrid (Sim)', 'Houston Sim']
            : ['SIGNAL-Bot Alpha', 'SIGNAL-Bot Beta', 'SIGNAL-Bot Gamma'];
        let idx = 0;
        for (const r of roles) {
          const taken = room.members.some((m) => m.roleId === r.id);
          if (!taken) {
            room.members.push({
              clientId: `ai-${r.id}`,
              name: `${simNames[idx % simNames.length]} · ${r.name}`,
              roleId: r.id,
              isAi: true,
              completedTaskIds: [],
            });
            idx++;
          }
        }
        saveRoomsToDisk();
        broadcastRoom(code);
        return;
      }

      if (msg.type === 'task:complete' && msg.taskId) {
        if (!room.completedTaskIds.includes(msg.taskId)) {
          room.completedTaskIds.push(msg.taskId);
          const actor = room.members.find(
            (m) => m.clientId === meta.clientId || (msg.roleId && m.roleId === msg.roleId)
          );
          if (actor && !actor.completedTaskIds.includes(msg.taskId)) {
            actor.completedTaskIds.push(msg.taskId);
          }

          if (msg.taskId.startsWith('role:') && actor && !actor.isAi) {
            const rId = msg.taskId.replace('role:', '');
            const roleMeta = MISSION_ROLES[room.missionId]?.find((r) => r.id === rId);
            room.messages.push({
              id: `done-${meta.clientId}-${rId}-${Date.now()}`,
              senderName: 'Mission Control',
              kind: 'system',
              text: `${actor.name}${actor.country ? ` (${actor.country})` : ''} completed ${roleMeta?.name || rId}’s station task!`,
              timestamp: Date.now(),
            });
          }

          const completedRolesCount = room.completedTaskIds.filter((t) =>
            t.startsWith('role:')
          ).length;
          const totalNeeded = MISSION_TASK_TOTALS[room.missionId] || 3;
          if (
            msg.taskId === 'shared:sequence' &&
            completedRolesCount >= totalNeeded
          ) {
            room.missionComplete = true;
            room.messages.push({
              id: `win-${Date.now()}`,
              senderName: 'Mission Control',
              kind: 'system',
              text: 'All 3 team roles and the shared mission sequence are verified! Mission complete.',
              timestamp: Date.now(),
            });
          }
          saveRoomsToDisk();
          broadcastRoom(code);
        }
        return;
      }

      if (msg.type === 'chat:send' && msg.text) {
        const cleanText = msg.text.trim().slice(0, 320);
        if (!cleanText) return;
        const msgId = msg.messageId || `chat-${meta.clientId}-${Date.now()}`;
        if (room.messages.some((m) => m.id === msgId)) return; // idempotent guard
        const member = room.members.find((m) => m.clientId === meta.clientId);
        const roleMeta = MISSION_ROLES[room.missionId]?.find((r) => r.id === member?.roleId);
        room.messages.push({
          id: msgId,
          senderName: member?.name || 'Specialist',
          senderRole: roleMeta?.name,
          senderCountry: member?.country,
          kind: 'player',
          text: cleanText,
          timestamp: Date.now(),
        });
        if (room.messages.length > 60) {
          room.messages = room.messages.slice(-60);
        }
        saveRoomsToDisk();
        broadcastRoom(code);

        // If player mentions @ai / @signal or is playing with AI Bots, respond in-room via SIGNAL Bot!
        const asksBot =
          /@(ai|signal|bot)\b/i.test(cleanText) ||
          (room.mode === 'solo_ai' && /\?/.test(cleanText));
        if (asksBot) {
          void generateTeamBotReply(room, cleanText, member?.name || 'Explorer').then((reply) => {
            const current = rooms.get(code);
            if (!current) return;
            current.messages.push({
              id: `ai-bot-${Date.now()}`,
              senderName: 'SIGNAL AI Bot',
              kind: 'ai_teammate',
              text: reply,
              timestamp: Date.now(),
            });
            saveRoomsToDisk();
            broadcastRoom(code);
          });
        }
        return;
      }

      if (msg.type === 'ai:ask' && msg.text) {
        const cleanText = msg.text.trim().slice(0, 320);
        if (!cleanText) return;
        const member = room.members.find((m) => m.clientId === meta.clientId);
        const roleMeta = MISSION_ROLES[room.missionId]?.find((r) => r.id === member?.roleId);
        room.messages.push({
          id: `ask-${meta.clientId}-${Date.now()}`,
          senderName: member?.name || 'Explorer',
          senderRole: roleMeta?.name,
          senderCountry: member?.country,
          kind: 'player',
          text: cleanText,
          timestamp: Date.now(),
        });
        saveRoomsToDisk();
        broadcastRoom(code);

        void generateTeamBotReply(room, cleanText, member?.name || 'Explorer').then((reply) => {
          const current = rooms.get(code);
          if (!current) return;
          current.messages.push({
            id: `ai-bot-${Date.now()}`,
            senderName: 'SIGNAL AI Bot',
            kind: 'ai_teammate',
            text: reply,
            timestamp: Date.now(),
          });
          saveRoomsToDisk();
          broadcastRoom(code);
        });
      }
    });

    ws.on('close', () => {
      const meta = socketMeta.get(ws);
      if (meta?.roomCode) {
        const sockets = roomSockets.get(meta.roomCode);
        sockets?.delete(ws);
        const room = rooms.get(meta.roomCode);
        if (room) {
          const departing = room.members.find((m) => m.clientId === meta.clientId);
          room.members = room.members.filter((m) => m.clientId !== meta.clientId);
          if (departing) {
            room.messages.push({
              id: `leave-${meta.clientId}-${Date.now()}`,
              senderName: 'Mission Control',
              kind: 'system',
              text: `${departing.name} disconnected.`,
              timestamp: Date.now(),
            });
            broadcastRoom(meta.roomCode);
          }
          // Prune abandoned empty universal queue rooms with 0 humans & 0 tasks
          if (
            room.mode === 'global_sim' &&
            room.members.filter((m) => !m.isAi).length === 0 &&
            room.completedTaskIds.length === 0
          ) {
            rooms.delete(meta.roomCode);
            roomSockets.delete(meta.roomCode);
          }
          saveRoomsToDisk();
        }
      }
      socketMeta.delete(ws);
    });
  });

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`Waiting for a Signal server listening on http://0.0.0.0:${PORT}`);
  });
}

void startServer();
