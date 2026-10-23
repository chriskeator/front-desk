/* Front Desk app: the claude.ai page db API (claude.use("db")) on top of Supabase table public.docs.
   Same calls the page already makes: collection().doc().get/set/update/delete, where/orderBy/limit, onSnapshot.
   Every write bumps docs.version (trigger). update() = read, merge, write only if the version is unchanged (retry on clash).
   Backend is swappable: DeskDB.make(backend) so tests can use an in-memory backend. */
(function () {
  "use strict";
  const isObj = v => v && typeof v === "object" && !Array.isArray(v);
  const clone = v => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
  function merge(a, b) {
    const out = Object.assign({}, a);
    for (const k of Object.keys(b)) {
      const v = b[k];
      if (v === undefined) continue;
      out[k] = isObj(v) && isObj(out[k]) ? merge(out[k], v) : clone(v);
    }
    return out;
  }
  const err = (code, message) => Object.assign(new Error(message), { code, message });
  const newId = () => { const c = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"; let s = ""; const r = crypto.getRandomValues(new Uint8Array(20)); for (const x of r) s += c[x % c.length]; return s; };
  const snapDoc = (id, data, pending) => ({ id, exists: data !== undefined, data: () => data === undefined ? undefined : clone(data), metadata: { fromCache: false, hasPendingWrites: !!pending } });
  function cmp(a, b) {
    if (typeof a === "number" && typeof b === "number") return a - b;
    if (typeof a === "boolean" && typeof b === "boolean") return (a ? 1 : 0) - (b ? 1 : 0);
    const x = typeof a === "string" ? a : JSON.stringify(a), y = typeof b === "string" ? b : JSON.stringify(b);
    return x < y ? -1 : x > y ? 1 : 0;
  }
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const OPS = {
    "==": (v, x) => eq(v, x), "!=": (v, x) => v !== undefined && !eq(v, x),
    "<": (v, x) => v !== undefined && v !== null && cmp(v, x) < 0, "<=": (v, x) => v !== undefined && v !== null && cmp(v, x) <= 0,
    ">": (v, x) => v !== undefined && v !== null && cmp(v, x) > 0, ">=": (v, x) => v !== undefined && v !== null && cmp(v, x) >= 0,
    "in": (v, x) => x.some(y => eq(v, y)), "not-in": (v, x) => v !== undefined && !x.some(y => eq(v, y)),
    "array-contains": (v, x) => Array.isArray(v) && v.some(y => eq(y, x))
  };

  function make(backend) {
    const cols = new Map();
    let emitQ = new Set(), emitT = 0;
    function col(name) {
      let c = cols.get(name);
      if (!c) {
        c = { map: new Map(), listeners: new Set(), loaded: false };
        c.ready = backend.load(name).then(rows => { for (const r of rows) c.map.set(r.id, { data: r.data || {}, version: r.version || 1 }); c.maxAt = maxAt(rows, ""); c.fullAt = Date.now(); c.loaded = true; });
        cols.set(name, c);
      }
      return c;
    }
    function emit(name) { emitQ.add(name); if (!emitT) emitT = setTimeout(() => { const q = emitQ; emitQ = new Set(); emitT = 0; for (const n of q) { const c = cols.get(n); if (c) for (const l of [...c.listeners]) l(); } }, 0); }
    function put(name, id, data, version, force) {
      const c = cols.get(name); if (!c) return;
      const cur = c.map.get(id);
      if (!force && cur && version != null && cur.version > version) return;
      c.map.set(id, { data, version: version != null ? version : (cur ? cur.version : 1) });
      emit(name);
    }
    function drop(name, id) { const c = cols.get(name); if (c && c.map.delete(id)) emit(name); }
    async function refreshDoc(name, id) {
      const c = cols.get(name); if (!c) return;
      const r = await backend.getOne(name, id);
      if (r) put(name, id, r.data || {}, r.version, true); else drop(name, id);
    }
    /* (Oct 23) Supabase free plan went over its 5.5 GB monthly bandwidth: every phone reopen re-downloaded every collection.
       Now a resync fetches only rows changed since the newest one we hold (updated_at), at most once per 15 s; a full reload
       (which also catches deletes missed while offline) only when the last one is over 6 hours old. */
    function maxAt(rows, cur) { let m = cur || ""; for (const r of rows) if (r.updated_at && r.updated_at > m) m = r.updated_at; return m; }
    let syncAt = 0;
    async function reloadAll() {
      if (Date.now() - syncAt < 15000) return; syncAt = Date.now();
      for (const [name, c] of cols) {
        if (!c.loaded) continue;
        try {
          if (!c.maxAt || Date.now() - (c.fullAt || 0) > 6 * 3600 * 1000) { const rows = await backend.load(name); c.map = new Map(rows.map(r => [r.id, { data: r.data || {}, version: r.version || 1 }])); c.maxAt = maxAt(rows, ""); c.fullAt = Date.now(); emit(name); }
          else { const rows = await backend.load(name, c.maxAt); for (const r of rows) put(name, r.id, r.data || {}, r.version); c.maxAt = maxAt(rows, c.maxAt); }
        } catch (e) {}
      }
    }
    if (backend.subscribe) backend.subscribe(e => {
      if (e.type === "delete") return drop(e.collection, e.id);
      if (e.data === undefined) return refreshDoc(e.collection, e.id).catch(() => {});
      put(e.collection, e.id, e.data, e.version);
    }, reloadAll);

    function query(name, filters, order, lim) {
      const run = () => {
        const c = col(name);
        let rows = [...c.map].map(([id, v]) => ({ id, data: v.data }));
        for (const [f, op, x] of filters) rows = rows.filter(r => OPS[op](r.data[f], x));
        if (order) {
          const [f, dir] = order, s = dir === "desc" ? -1 : 1;
          rows.sort((a, b) => {
            const x = a.data[f], y = b.data[f], mx = x === undefined || x === null, my = y === undefined || y === null;
            if (mx || my) return mx && my ? cmp(a.id, b.id) : mx ? 1 : -1;
            return s * cmp(x, y) || cmp(a.id, b.id);
          });
        } else rows.sort((a, b) => cmp(a.id, b.id));
        if (lim) rows = rows.slice(0, lim);
        const docs = rows.map(r => snapDoc(r.id, r.data));
        return { docs, size: docs.length, empty: !docs.length, docChanges: () => docs.map((d, i) => ({ type: "added", doc: d, oldIndex: -1, newIndex: i })), metadata: { fromCache: false, hasPendingWrites: false } };
      };
      const q = {
        where(f, op, x) { if (!OPS[op]) throw new TypeError("bad operator " + op); return query(name, filters.concat([[f, op, x]]), order, lim); },
        orderBy(f, dir) { return query(name, filters, [f, dir || "asc"], lim); },
        limit(n) { return query(name, filters, order, n); },
        async get() { await col(name).ready; return run(); },
        onSnapshot(next, error) {
          const c = col(name); let live = true;
          const fire = () => { if (live) try { next(run()); } catch (e) { setTimeout(() => { throw e; }); } };
          c.ready.then(() => { if (!live) return; c.listeners.add(fire); fire(); },
            e => { if (live && error) error({ code: "unavailable", message: String(e && e.message || e) }); });
          return () => { live = false; c.listeners.delete(fire); };
        }
      };
      return q;
    }
    function docRef(name, id) {
      return {
        id, path: name + "/" + id,
        async get() { const r = await backend.getOne(name, id); return snapDoc(id, r ? r.data || {} : undefined); },
        async set(data) {
          if (!isObj(data)) throw err("invalid_argument", "set needs an object");
          const body = clone(data), c = cols.get(name), before = c && c.map.get(id);
          put(name, id, body, before ? before.version : 0, true);
          try { const v = await backend.upsert(name, id, body); put(name, id, body, v, true); }
          catch (e) { if (c) { if (before) put(name, id, before.data, before.version, true); else drop(name, id); } throw err("unavailable", String(e && e.message || e)); }
        },
        async update(data) {
          if (!isObj(data)) throw err("invalid_argument", "update needs an object");
          const c = cols.get(name), before = c && c.map.get(id);
          if (before) put(name, id, merge(before.data, data), before.version, true);
          try {
            for (let i = 0; i < 5; i++) {
              const r = await backend.getOne(name, id);
              if (!r) throw err("invalid_argument", "no such document " + name + "/" + id);
              const body = merge(r.data || {}, data);
              const v = await backend.updateIf(name, id, body, r.version);
              if (v != null) { put(name, id, body, v, true); return; }
            }
            throw err("unavailable", "kept clashing with another write");
          } catch (e) {
            if (before) refreshDoc(name, id).catch(() => {});
            throw e.code ? e : err("unavailable", String(e && e.message || e));
          }
        },
        async delete() {
          const c = cols.get(name), before = c && c.map.get(id);
          drop(name, id);
          try { await backend.remove(name, id); } catch (e) { if (before) put(name, id, before.data, before.version, true); throw err("unavailable", String(e && e.message || e)); }
        },
        onSnapshot(next, error) {
          return query(name, [], null, 0).onSnapshot(s => { const d = s.docs.find(x => x.id === id); next(d || snapDoc(id, undefined)); }, error);
        },
        collection(sub) { return collRef(name + "/" + id + "/" + sub); }
      };
    }
    function collRef(name) {
      const q = query(name, [], null, 0);
      return Object.assign(q, { path: name, doc: id => docRef(name, id == null ? newId() : String(id)), async add(data) { const r = docRef(name, newId()); await r.set(data); return r; } });
    }
    return {
      collection: name => collRef(name),
      doc: path => { const p = path.split("/"); if (p.length % 2) throw new TypeError("doc path needs an even number of segments"); return docRef(p.slice(0, -1).join("/"), p[p.length - 1]); },
      _reloadAll: reloadAll
    };
  }

  /* (Oct 15) TEST copy (test.html sets window.DESK_TESTCOPY): the page's names map to test_<name>; any other name is refused,
     so test.html can never read or write a live collection. Realtime events from live rows are ignored. */
  const TESTP = "test_";
  function testOnly(name) { if (typeof name !== "string" || name.indexOf(TESTP) !== 0) throw err("permission_denied", "Test copy: refused live collection " + name); return name; }
  function testBackend(inner) {
    const m = n => testOnly(TESTP + n);
    return {
      load: (n, since) => inner.load(m(n), since), getOne: (n, id) => inner.getOne(m(n), id), upsert: (n, id, d) => inner.upsert(m(n), id, d),
      updateIf: (n, id, d, v) => inner.updateIf(m(n), id, d, v), remove: (n, id) => inner.remove(m(n), id),
      subscribe: (onEvt, onResync) => inner.subscribe(e => { if (e.collection && e.collection.indexOf(TESTP) === 0) onEvt(Object.assign({}, e, { collection: e.collection.slice(TESTP.length) })); }, onResync)
    };
  }

  function supabaseBackend(sb) {
    const T0 = () => sb.from("docs");
    const T = () => { const q = T0(); if (!window.DESK_TESTCOPY) return q;
      return { select: (...a) => { const r = q.select(...a); const eq0 = r.eq.bind(r); r.eq = (c, v) => { if (c === "collection") testOnly(v); return eq0(c, v); }; return r; },
        upsert: (row, o) => { testOnly(row && row.collection); return q.upsert(row, o); },
        update: (d) => { const r = q.update(d); const eq0 = r.eq.bind(r); r.eq = (c, v) => { if (c === "collection") testOnly(v); return eq0(c, v); }; return r; },
        delete: () => { const r = q.delete(); const eq0 = r.eq.bind(r); r.eq = (c, v) => { if (c === "collection") testOnly(v); return eq0(c, v); }; return r; } }; };
    const chk = ({ data, error }) => { if (error) throw error; return data; };
    return {
      async load(name, since) {
        const out = [];
        for (let from = 0; ; from += 1000) {
          let q = T().select("id,data,version,updated_at").eq("collection", name); if (since) q = q.gt("updated_at", since);
          const d = chk(await q.order("id").range(from, from + 999));
          out.push(...d); if (d.length < 1000) break;
        }
        return out;
      },
      async getOne(name, id) { return chk(await T().select("id,data,version").eq("collection", name).eq("id", id).maybeSingle()); },
      async upsert(name, id, data) { return chk(await T().upsert({ collection: name, id, data }, { onConflict: "collection,id" }).select("version").single()).version; },
      async updateIf(name, id, data, version) { const d = chk(await T().update({ data }).eq("collection", name).eq("id", id).eq("version", version).select("version")); return d.length ? d[0].version : null; },
      async remove(name, id) { chk(await T().delete().eq("collection", name).eq("id", id)); },
      subscribe(onEvt, onResync) {
        let ok = false;
        sb.channel("desk-docs").on("postgres_changes", { event: "*", schema: "public", table: "docs" }, p => {
          if (p.eventType === "DELETE") { if (p.old && p.old.collection) onEvt({ type: "delete", collection: p.old.collection, id: p.old.id }); else onResync(); }
          else if (p.new) onEvt({ type: "put", collection: p.new.collection, id: p.new.id, data: p.new.data, version: p.new.version });
        }).subscribe(status => { const was = ok; ok = status === "SUBSCRIBED"; if (ok && !was) onResync(); });
        document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") onResync(); });
        setInterval(() => { if (!ok && document.visibilityState === "visible") onResync(); }, 30000);
      }
    };
  }

  const downloads = {
    async save({ filename, data }) {
      const blob = data instanceof Blob ? data : new Blob([data], { type: "text/plain" });
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = filename || "download";
      document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
    }
  };

  window.DeskDB = { make, merge, supabaseBackend, testBackend, testOnly };
  if (window.DESK_TEST) return;   /* the test page sets window.claude itself */

  const CFG = window.DESK_CFG;
  let dbP = null;
  async function openDb() {
    const sb = window.supabase.createClient(CFG.url, CFG.key);
    if (!window.DESK_TESTCOPY) window.deskSb = sb;
    const { data } = await sb.auth.getSession();
    if (!data.session) { location.replace("./"); return null; }
    try { sb.realtime.setAuth(data.session.access_token); } catch (e) {}
    sb.auth.onAuthStateChange((ev, s) => {
      if (s && s.access_token) try { sb.realtime.setAuth(s.access_token); } catch (e) {}
      if (ev === "SIGNED_OUT") location.replace("./");
    });
    return make(window.DESK_TESTCOPY ? testBackend(supabaseBackend(sb)) : supabaseBackend(sb));
  }
  window.claude = { use: async name => name === "db" ? (dbP || (dbP = openDb())) : name === "downloads" ? downloads : null };
})();
