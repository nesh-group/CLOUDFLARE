// =====================================================================
// BANDWIDTH-OPTIMISED VERSION — behaviour identical, far less Firebase download.
//  1. Cron sweep no longer downloads rides + drivers + ALL restaurants every
//     minute. It reads a tiny `dispatch_queue` node (written when a nearest-
//     driver offer goes out) and returns at once when it is empty.
//  2. Single fields (fcmToken, goSeries, location, name) are read directly
//     instead of whole restaurants / customers / drivers nodes.
//  3. Driver lists are fetched online-only (needs .indexOn isOnline; falls
//     back to the old full read automatically if the index is missing).
//  4. Google access token cached for 50 min instead of re-signed every call.
// Optional Firebase rules:  taxi_drivers: ".indexOn": ["isOnline"]
//                           loyalty:      ".indexOn": ["claimedAt"]
// =====================================================================
// ---------------------------------------------------------------------
// COMBINED WORKER — replaces driver_index.js AND partner_index.js.
// There is only ever ONE Cloudflare Worker for the whole system (it's
// called by the customer app, the driver app, and the partner app alike)
// — the two uploaded files were just slightly out-of-sync copies of the
// same worker. This file is the newer, more complete one (it treats
// delivery-job alerts as full-screen "ringing" alerts too, not just
// plain tray notifications), kept as the single source of truth. Deploy
// ONLY this file — do not deploy driver_index.js or partner_index.js.
// ---------------------------------------------------------------------
//
// CLOUDFLARE WORKER — sends order-alert AND ride-alert push notifications,
// with Uber-style NEAREST-DRIVER-FIRST dispatch for rides + deliveries.
//
// This replaces Firebase Cloud Functions (which needs the card-requiring
// Blaze plan). Cloudflare Workers' free tier (100,000 requests/day) does
// NOT require a card to sign up. This worker is the only "server" piece
// in the whole system — everything else is free client-side code.
//
// WHAT IT DOES:
// - The customer app calls this worker right after placing an order
//   (type: "order") — looks up the restaurant's FCM token under
//   restaurants/{restId} and sends a push. (Unchanged — single fixed
//   target, no "nearest driver" concept applies here.)
// - The taxi-booking side of the customer app calls this worker right
//   after a ride is created (type: "ride"). Instead of broadcasting to
//   every online driver at once, this now finds the SINGLE nearest
//   online driver (by straight-line distance from the driver's last
//   known GPS location to the ride's pickup point) and pushes to just
//   that one — exactly like Uber's first-offer-to-closest-driver model.
// - Same nearest-first idea for delivery jobs (type: "delivery").
// - If nobody accepts within NEAREST_TIMEOUT_MS, the `scheduled` Cron
//   Trigger below (runs every minute — see DEPLOY step 4) notices the
//   ride/order is still unclaimed and broadcasts to every remaining
//   online driver within ESCALATE_RADIUS_KM, same as the old behaviour.
//   Whichever driver accepts first still wins either way.
// Either way, this worker calls Google's FCM HTTP v1 API directly with
// the custom alarm sound + high-priority flag. Even if the app is fully
// closed/killed, Android's own Google Play Services (not your app)
// receives this push and wakes the app.
//
// DEPLOY (from phone browser, no computer needed):
// 1. Go to dash.cloudflare.com, sign up free (no card).
// 2. Workers & Pages → Create → Create Worker → paste this file's code in
//    the online editor → Deploy. You'll get a URL like
//    https://order-alert.YOUR-SUBDOMAIN.workers.dev
// 3. In the worker's Settings → Variables, add these (see SETUP below):
//    - FIREBASE_DB_URL
//    - FIREBASE_SERVICE_ACCOUNT_JSON  (paste the whole JSON key file
//      contents as one variable — mark it "Encrypt")
// 4. NEW — for the nearest→broadcast escalation to fire on its own:
//    Worker → Settings → Triggers → Cron Triggers → Add → schedule
//    `* * * * *` (every minute — Cloudflare's free-tier minimum
//    granularity, so escalation kicks in roughly 25–85s after the
//    nearest driver was first notified, not on-the-dot at 25s).
//
// CALLING IT:
// Restaurant order alert (unchanged from before):
//   POST { restId, orderNumber, itemsSummary, customerName, customerPhone,
//          customerAddress, totalAmount }
//   ("type" omitted or "order" — kept backward compatible with the
//   existing customer app, which doesn't send "type" at all.)
//
// Driver ride alert (nearest-first, then broadcast on timeout):
//   POST { type: "ride", vehicleType, rideId, pickupLat, pickupLng,
//          pickupAddress, dropAddress, customerName, customerPhone, fare }
//   pickupLat/pickupLng are required to find the nearest driver — if
//   they're missing, or no online driver has a recent location on file
//   yet, this falls back to the old immediate broadcast so nothing
//   breaks for drivers who haven't updated their app.
//
// Driver delivery-job alert (nearest-first, then broadcast on timeout):
//   POST { type: "delivery", restId, orderId, restName, orderNumber,
//          itemsSummary, customerName, customerPhone, customerAddress,
//          totalAmount, deliveryFee }
//   Sent by the partner app right after a shop marks an order "ready".
//   The restaurant's own saved location (restaurants/{restId}.location)
//   is used as the pickup point to find the nearest driver — no change
//   needed on the partner app's side.
// ---------------------------------------------------------------------

// How long the nearest driver alone gets before we widen to everyone
// nearby. Real escalation timing is bounded below by the Cron Trigger's
// minute-level granularity (see `scheduled` below), not this constant.
// Applies to taxi rides.
const NEAREST_TIMEOUT_MS = 25 * 1000;
// Same idea, but for Go Series delivery jobs specifically — kept separate
// so delivery's nearest-only window can be tuned without touching taxi
// rides. Mirrors DELIVERY_NEAREST_WINDOW_MS in driver.html.
const DELIVERY_NEAREST_TIMEOUT_MS = 2 * 60 * 1000;
// Radius used both for picking the nearest driver and (mainly) for the
// broadcast fallback — drivers further than this are assumed too far to
// bother alerting.
const MAX_MATCH_RADIUS_KM = 12;
// A driver's last GPS ping older than this is treated as stale/offline
// even if their isOnline flag says otherwise (e.g. app was killed).
const LOCATION_STALE_MS = 3 * 60 * 1000;

// Alert types that get the full-screen "ringing" treatment (data-only FCM
// message so each app's own FirebaseMessagingService fires even when the
// app is fully killed, letting it show a native full-screen incoming-alert
// screen instead of a plain system-tray notification). Shared by both the
// main request handler and the Cron escalation sweep below.
const FULL_SCREEN_ALERT_TYPES = new Set(['new_ride', 'new_order', 'new_delivery', 'new_operator_order']);

// Data-only alerts that must reach the app even when fully killed, but are
// NEVER shown to the person at all — no ring, no tray notification. Right
// now this is just the OTP-SMS relay: the customer app never talks to an
// SMS provider directly, so it asks this worker to silently hand the code
// to whichever shop's device is currently flagged as the "OTP sender" (see
// otp_sender_shop in Firebase, set from the Operator Panel), and that
// device's AlertFcmService sends the actual SMS from its own SIM.
const SILENT_DATA_TYPES = new Set(['send_otp_sms']);

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Parses the "lat,lng" text format used for restaurants/{id}.location.
function parseLatLngStr(str) {
  if (!str) return null;
  const m = String(str).trim().match(/^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$/);
  if (!m) return null;
  return { lat: parseFloat(m[1]), lng: parseFloat(m[2]) };
}

// ---------------------------------------------------------------------
// BANDWIDTH HELPERS (added) — the worker used to download whole Firebase
// nodes (e.g. restaurants/{id} = menu + every past order) just to read one
// field. These read ONLY the field that is needed. Same data, same results.
// ---------------------------------------------------------------------
async function rtdbGet(dbUrl, path, accessToken) {
  try {
    const r = await fetch(`${dbUrl}/${path}.json`, accessToken ? { headers: { Authorization: `Bearer ${accessToken}` } } : undefined);
    if (!r.ok) return null;
    return await r.json();
  } catch (e) { return null; }
}
async function rtdbFields(dbUrl, basePath, fields, accessToken) {
  const vals = await Promise.all(fields.map((f) => rtdbGet(dbUrl, `${basePath}/${f}`, accessToken)));
  const out = {};
  fields.forEach((f, i) => { out[f] = vals[i]; });
  return out;
}
// Online drivers only. Needs  "taxi_drivers": { ".indexOn": ["isOnline"] }  in the
// Firebase rules; if that index is missing (or the query fails for any reason) this
// silently falls back to the old full download, so nothing can break.
let _onlineQueryWorks = true;
async function fetchOnlineDrivers(dbUrl) {
  if (_onlineQueryWorks) {
    try {
      const r = await fetch(`${dbUrl}/taxi_drivers.json?orderBy=%22isOnline%22&equalTo=true`);
      if (r.ok) {
        const j = await r.json();
        if (!(j && j.error)) return j || {};
      }
      _onlineQueryWorks = false;
    } catch (e) { _onlineQueryWorks = false; }
  }
  const r2 = await fetch(`${dbUrl}/taxi_drivers.json`);
  return (await r2.json()) || {};
}
async function rtdbNotifiedIds(dbUrl, nodePath) {
  const v = await rtdbGet(dbUrl, `${nodePath}/dispatch/notifiedIds`);
  if (Array.isArray(v)) return v;
  if (v && typeof v === 'object') return Object.values(v);
  return [];
}
// FCM tokens for a list of driver ids. Few ids -> read just their token field;
// many ids -> one full read (keeps us far below Cloudflare's subrequest limit).
async function driverTokensFor(dbUrl, ids) {
  if (!ids.length) return [];
  if (ids.length <= 10) {
    const toks = await Promise.all(ids.map((id) => rtdbGet(dbUrl, `taxi_drivers/${id}/fcmToken`)));
    return toks.filter(Boolean);
  }
  const r = await fetch(`${dbUrl}/taxi_drivers.json`);
  const drivers = (await r.json()) || {};
  return ids.map((id) => drivers[id] && drivers[id].fcmToken).filter(Boolean);
}

// Ranks online, matching drivers by distance to a pickup point. Drivers
// with no location, or a location older than LOCATION_STALE_MS, are
// excluded from ranking (we simply don't know where they are).
function rankDriversByDistance(drivers, pickup, filterFn) {
  const now = Date.now();
  return Object.entries(drivers)
    .filter(([id, d]) => d && filterFn(d))
    .map(([id, d]) => {
      const loc = d.location;
      const hasFreshLoc = loc && typeof loc.lat === 'number' && typeof loc.lng === 'number' &&
        loc.updatedAt && (now - loc.updatedAt) < LOCATION_STALE_MS;
      const distanceKm = hasFreshLoc ? haversineKm(pickup.lat, pickup.lng, loc.lat, loc.lng) : null;
      return { id, driver: d, distanceKm };
    })
    .sort((a, b) => {
      if (a.distanceKm == null && b.distanceKm == null) return 0;
      if (a.distanceKm == null) return 1;
      if (b.distanceKm == null) return -1;
      return a.distanceKm - b.distanceKm;
    });
}

// Alerts every device that has opened the Operator Panel (operator_devices/{key}).
//  - NEW app (device saved fs:2): data-only push -> app shows a FULL-SCREEN, SILENT alert.
//  - OLD app (no fs flag): gets NOTHING. An old app has no silent channel, so any
//    notification we send would make Android play a sound. Silence > sound, so they
//    are skipped until they update the APK (they still see orders inside the panel).
// Best-effort: failures are swallowed so they can never break the shop's own order alert.
async function notifyOperatorDevices(env, { title, body, orderId }) {
  try {
    const dbUrl = env.FIREBASE_DB_URL.replace(/\/$/, '');
    const devices = (await (await fetch(`${dbUrl}/operator_devices.json`)).json()) || {};
    const list = Object.keys(devices).map((k) => ({ key: k, token: devices[k] && devices[k].token, fs: devices[k] && devices[k].fs, sound: devices[k] && devices[k].sound, vibrate: devices[k] && devices[k].vibrate })).filter((d) => d.token && d.fs >= 2); // old apps skipped on purpose (see above)
    if (!list.length) return;
    const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
    const accessToken = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
    await Promise.all(list.map(async (d) => {
      const message = {
        token: d.token,
        data: { type: 'new_operator_order', orderId: orderId || '', title: String(title || ''), body: String(body || ''), sound: d.sound ? '1' : '0', vibrate: d.vibrate ? '1' : '0' },
        android: { priority: 'high' },
      };
      const res = await fetch(`https://fcm.googleapis.com/v1/projects/${serviceAccount.project_id}/messages:send`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message }),
      });
      if (!res.ok) {
        const r = await res.json().catch(() => ({}));
        const err = r && r.error;
        const dead = err && (err.status === 'NOT_FOUND' || err.status === 'UNREGISTERED' ||
          (Array.isArray(err.details) && err.details.some((x) => x.errorCode === 'UNREGISTERED')));
        if (dead) await fetch(`${dbUrl}/operator_devices/${d.key}.json`, { method: 'DELETE', headers: { Authorization: `Bearer ${accessToken}` } }).catch(() => {});
      }
    }));
  } catch (e) { /* never block the order alert */ }
}

export default {
  async fetch(request, env) {
    // CORS: lets the customer/driver apps (running in a browser) call
    // this Worker from a different domain. Without this, browsers block
    // the request before it reaches the code below.
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    if (request.method !== 'POST') {
      return new Response('Use POST', { status: 405, headers: corsHeaders });
    }

    let body;
    try {
      body = await request.json();
    } catch (e) {
      return new Response('Invalid JSON', { status: 400, headers: corsHeaders });
    }

    // ---------------------------------------------------------------------
    // TOPIC SUBSCRIBE — a customer device registers its FCM token and asks
    // to be subscribed to the "all_customers" topic, so a single broadcast
    // send (below) reaches every customer without looking up every token
    // individually. Called from index.html right after messaging.getToken()
    // succeeds — for both logged-in customers and guests (guests just don't
    // also get a customers/{phone}/fcmToken save).
    // ---------------------------------------------------------------------
    if (body.type === 'subscribe_topic') {
      const { token } = body;
      if (!token) return new Response('token required', { status: 400, headers: corsHeaders });
      // NEW: optional zone topics. Without them -> old behaviour (all_customers only).
      //   topics: ['zone_<id>', ...]       -> also subscribe to these zone topics
      //   unsubscribeTopics: ['zone_<id>'] -> leave these zone topics (customer moved to another zone)
      // Only "zone_" topics are accepted so nobody can mess with other topics.
      const okTopic = t => typeof t === 'string' && /^zone_[A-Za-z0-9_\-.~%]{1,100}$/.test(t);
      const addTopics = Array.isArray(body.topics) ? body.topics.filter(okTopic).slice(0, 10) : [];
      const delTopics = Array.isArray(body.unsubscribeTopics) ? body.unsubscribeTopics.filter(okTopic).slice(0, 10) : [];
      const accessToken2 = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
      // The legacy Instance ID (IID) topic-management API defaults to
      // expecting a legacy FCM server key in Authorization. To use an OAuth
      // bearer token (a service account access token, as we do here) instead,
      // Google requires this extra header — without it the request comes
      // back 401/403 and the device silently never actually joins
      // "all_customers", even though this fetch's caller (index.html) never
      // checks the response and shows no error either. This is the reason
      // broadcasts from the Operator Panel were reaching nobody.
      const iidHeaders = { Authorization: `Bearer ${accessToken2}`, access_token_auth: 'true' };
      const subRes = await fetch(
        `https://iid.googleapis.com/iid/v1/${token}/rel/topics/all_customers`,
        { method: 'POST', headers: iidHeaders }
      );
      let zoneOk = true;
      for (const t of addTopics) {
        const r = await fetch(`https://iid.googleapis.com/iid/v1/${token}/rel/topics/${t}`, { method: 'POST', headers: iidHeaders });
        if (!r.ok) zoneOk = false;
      }
      for (const t of delTopics) {
        const r = await fetch('https://iid.googleapis.com/iid/v1:batchRemove', {
          method: 'POST',
          headers: { ...iidHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({ to: '/topics/' + t, registration_tokens: [token] }),
        });
        if (!r.ok) zoneOk = false;
      }
      const allOk = subRes.ok && zoneOk;
      return new Response(JSON.stringify({ ok: allOk }), {
        status: allOk ? 200 : 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // ---------------------------------------------------------------------
    // BROADCAST — Operator Panel sends one announcement/offer to every
    // customer subscribed to "all_customers", in a single FCM call
    // (logged-in customers AND guests who granted notification permission).
    // ---------------------------------------------------------------------
    if (body.type === 'broadcast') {
      const { title, body: msgBody, image } = body;
      if (!title || !msgBody) return new Response('title and body required', { status: 400, headers: corsHeaders });
      // NEW: optional zoneIds -> send ONLY to customers whose last location is in those zones
      // (they are subscribed to topic "zone_<id>"). No zoneIds -> everyone ("all_customers"), as before.
      const zoneIds = Array.isArray(body.zoneIds)
        ? body.zoneIds.filter(z => typeof z === 'string' && /^[A-Za-z0-9_\-.~%]{1,100}$/.test(z)).slice(0, 20)
        : [];
      const serviceAccount2 = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
      const projectId2 = serviceAccount2.project_id;
      const accessToken2 = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
      const notificationPayload = image
        ? { title, body: msgBody, image }
        : { title, body: msgBody };
      const sendToTopic = (topicName) => fetch(
        `https://fcm.googleapis.com/v1/projects/${projectId2}/messages:send`,
        {
          method: 'POST',
          headers: { Authorization: `Bearer ${accessToken2}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: {
              topic: topicName,
              notification: notificationPayload,
              android: { priority: 'high', notification: notificationPayload },
            },
          }),
        }
      );
      const topicList = zoneIds.length ? zoneIds.map(z => 'zone_' + z) : ['all_customers'];
      const fcmResults = await Promise.all(topicList.map(async t => {
        const r = await sendToTopic(t);
        return { topic: t, ok: r.ok, result: await r.json().catch(() => ({})) };
      }));
      const allSent = fcmResults.every(r => r.ok);
      return new Response(JSON.stringify({ ok: allSent, result: fcmResults }), {
        status: allSent ? 200 : 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // ---------------------------------------------------------------------
    // CANCEL RING — tells whichever device(s) are currently ringing for a
    // specific ride/delivery/order to stop, right away, instead of waiting
    // out RingingService's own 25s AUTO_STOP_MS timeout (driver side) or
    // ringing indefinitely (restaurant order side, which has no timeout at
    // all). This must be handled here, BEFORE the notifType fallback below
    // — previously a { type: 'cancel_ring', restId, orderId } call (the
    // only caller today, in index.html's order-cancel flow) fell through
    // to the `: 'order'` default and was sent out as a brand-new "New
    // Order" alert to the restaurant, re-ringing the exact thing it was
    // meant to silence.
    //
    // kind is optional for backward compatibility with that existing
    // caller, which never sends it: rideId present -> 'ride', otherwise
    // (restId/orderId only) -> 'order'. New callers (e.g. the taxi
    // "Stop searching" flow) should pass kind explicitly.
    if (body.type === 'cancel_ring') {
      const { kind, rideId, restId, orderId } = body;
      const resolvedKind = kind || (rideId ? 'ride' : 'order');
      const dbUrlC = env.FIREBASE_DB_URL.replace(/\/$/, '');
      const serviceAccountC = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
      const projectIdC = serviceAccountC.project_id;

      let targetTokens = [];
      let alertId = '';

      if (resolvedKind === 'ride') {
        if (!rideId) return new Response('rideId required', { status: 400, headers: corsHeaders });
        alertId = rideId;
        // Whoever this ride's most recent dispatch actually notified
        // (nearest driver, or the widened broadcast list) is who might
        // still be ringing — look those tokens up rather than guessing.
        const notifiedIds = await rtdbNotifiedIds(dbUrlC, `rides/${rideId}`);
        targetTokens = await driverTokensFor(dbUrlC, notifiedIds);
      } else if (resolvedKind === 'delivery') {
        if (!restId || !orderId) return new Response('restId and orderId required', { status: 400, headers: corsHeaders });
        alertId = `${restId}::${orderId}`;
        const notifiedIds = await rtdbNotifiedIds(dbUrlC, `restaurants/${restId}/orders/${orderId}`);
        targetTokens = await driverTokensFor(dbUrlC, notifiedIds);
      } else {
        // 'order' — single fixed target, the restaurant's own saved token.
        if (!restId) return new Response('restId required', { status: 400, headers: corsHeaders });
        alertId = orderId || '';
        const restToken = await rtdbGet(dbUrlC, `restaurants/${restId}/fcmToken`);
        if (restToken) targetTokens = [restToken];
      }

      if (!targetTokens.length) {
        return new Response(JSON.stringify({ ok: true, sent: 0 }), {
          status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }

      const accessTokenC = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
      // Data-only, silent (no "notification" key, not in FULL_SCREEN_ALERT_TYPES)
      // — this must never itself pop a tray notification or ring anything;
      // it only carries an instruction for AlertFcmService to act on.
      await Promise.all(targetTokens.map(fcmToken =>
        fetch(`https://fcm.googleapis.com/v1/projects/${projectIdC}/messages:send`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${accessTokenC}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: {
              token: fcmToken,
              data: { type: 'cancel_ring', kind: resolvedKind, id: alertId },
              android: { priority: 'high' },
            },
          }),
        }).catch(() => {})
      ));

      return new Response(JSON.stringify({ ok: true, sent: targetTokens.length }), {
        status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    const notifType = body.type === 'ride' ? 'ride' : (body.type === 'delivery' ? 'delivery' : (body.type === 'delivery_ready' ? 'delivery_ready' : (body.type === 'customer' ? 'customer' : (body.type === 'otp' ? 'otp' : (body.type === 'operator' ? 'operator' : 'order')))));

    const dbUrl = env.FIREBASE_DB_URL.replace(/\/$/, '');
    const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
    const projectId = serviceAccount.project_id;

    let tokens, notifTitle, notifBody, dataPayload, channelId, soundName;
    // Filled in for 'ride'/'delivery' so we can write dispatch metadata
    // (who was notified, and when) after the push goes out — the Cron
    // Trigger's `scheduled` handler reads this back to decide when to
    // widen from "nearest driver only" to a full broadcast.
    let dispatchWrite = null; // { path, body }
    let operatorKeyByToken = null; // filled for 'operator' alerts so dead tokens can be cleaned up

    if (notifType === 'ride') {
      // ---------------- RIDE ALERT (driver app, nearest-first) ----------------
      const {
        rideId,
        vehicleType,
        pickupLat,
        pickupLng,
        pickupAddress,
        dropAddress,
        customerName,
        customerPhone,
        fare,
        driverId, // Choice Driver flow (index.html) — customer picked this
                  // driver by hand, so skip nearest-ranking/broadcast
                  // entirely and ring only them. No dispatchWrite either:
                  // the Choice Driver flow has its own 25s timeout that
                  // clears chosenDriverId, it never widens to other drivers.
      } = body;

      if (driverId) {
        const driverTok = await rtdbGet(dbUrl, `taxi_drivers/${driverId}/fcmToken`);
        tokens = driverTok ? [driverTok] : [];
        if (!tokens.length) {
          return new Response('No FCM token for this driver', { status: 200, headers: corsHeaders });
        }
      } else {
        if (!vehicleType) return new Response('vehicleType required', { status: 400, headers: corsHeaders });

        const drivers = await fetchOnlineDrivers(dbUrl);
        const matchFilter = d => d.isOnline === true && d.vehicleType === vehicleType && d.fcmToken && (!d.services || d.services.taxi !== false);

        let chosen; // array of { id, driver, distanceKm }
        const havePickup = typeof pickupLat === 'number' && typeof pickupLng === 'number';
        if (havePickup) {
          const ranked = rankDriversByDistance(drivers, { lat: pickupLat, lng: pickupLng }, matchFilter);
          const withinRadius = ranked.filter(r => r.distanceKm == null || r.distanceKm <= MAX_MATCH_RADIUS_KM);
          // Nearest driver with a known fresh location — if nobody has a
          // location on file yet, fall back to the old broadcast-to-all
          // behaviour so this never silently strands a ride.
          const nearest = withinRadius.find(r => r.distanceKm != null);
          chosen = nearest ? [nearest] : withinRadius;
        } else {
          chosen = Object.entries(drivers).filter(([id, d]) => d && matchFilter(d)).map(([id, d]) => ({ id, driver: d, distanceKm: null }));
        }

        tokens = chosen.map(c => c.driver.fcmToken).filter(Boolean);
        if (!tokens.length) {
          return new Response('No online drivers with a token for this vehicle type', { status: 200, headers: corsHeaders });
        }

        if (rideId && havePickup && chosen.length === 1 && chosen[0].distanceKm != null) {
          dispatchWrite = {
            path: `rides/${rideId}`,
            body: { dispatch: { stage: 'nearest', notifiedIds: chosen.map(c => c.id), notifiedAt: Date.now(), pickupLat, pickupLng } },
            // tiny "to-do" entry so the Cron sweep never has to scan every ride/restaurant
            queue: { key: 'r_' + rideId, value: { kind: 'ride', rideId, notifiedAt: Date.now() } },
          };
        }
      }

      notifTitle = 'New Ride Request' + (customerName ? ' — ' + customerName : '');

      const bodyLines = [];
      if (pickupAddress) bodyLines.push('Pickup: ' + pickupAddress);
      if (dropAddress) bodyLines.push('Drop: ' + dropAddress);
      if (fare) bodyLines.push('Fare: ₹' + fare);
      if (customerPhone) bodyLines.push('Ph: ' + customerPhone);
      notifBody = bodyLines.length ? bodyLines.join(' · ') : 'Tap to view ride details';

      dataPayload = {
        type: 'new_ride',
        rideId: rideId || '',
        pickupAddress: pickupAddress || '',
        dropAddress: dropAddress || '',
        customerName: customerName || '',
        customerPhone: customerPhone || '',
        fare: fare != null ? String(fare) : '',
        vehicleType: vehicleType || '',
      };

      channelId = 'ride_alerts';
      soundName = 'ride_alert';
    } else if (notifType === 'delivery') {
      // ---------------- DELIVERY JOB ALERT (driver app, nearest-first) ----------------
      // A shop marked an order "ready". The restaurant's own saved
      // location is used as the pickup point so the nearest available
      // driver (any vehicle type — delivery isn't restricted the way
      // taxi rides are) gets offered the job first.
      const {
        restId,
        orderId,
        restName,
        orderNumber,
        itemsSummary,
        customerName,
        customerPhone,
        customerAddress,
        totalAmount,
        deliveryFee,
      } = body;
      if (!restId || !orderId) return new Response('restId and orderId required', { status: 400, headers: corsHeaders });

      const [drivers, rest] = await Promise.all([
        fetchOnlineDrivers(dbUrl),
        rtdbFields(dbUrl, `restaurants/${restId}`, ['goSeries', 'location']),
      ]);
      // Only Go Series shops (Operator Panel → Go Series) route orders to
      // bike delivery boys — every other shop arranges its own delivery, so
      // it's never worth pinging a driver's phone for one. This mirrors
      // partner.html's own client-side check; kept here too so this stays
      // true no matter what calls this endpoint.
      if (!rest.goSeries) {
        return new Response('Shop is not in Go Series — not routed to delivery drivers', { status: 200, headers: corsHeaders });
      }
      const pickup = parseLatLngStr(rest.location);
      // Bike drivers only — matches driver.html's driverDoesDelivery(): auto/cab
      // drivers never see delivery jobs in the app, so never ring them either.
      const matchFilter = d => d.isOnline === true && d.vehicleType === 'bike' && d.fcmToken && (!d.services || d.services.delivery !== false);

      let chosen;
      if (pickup) {
        const ranked = rankDriversByDistance(drivers, pickup, matchFilter);
        const withinRadius = ranked.filter(r => r.distanceKm == null || r.distanceKm <= MAX_MATCH_RADIUS_KM);
        const nearest = withinRadius.find(r => r.distanceKm != null);
        chosen = nearest ? [nearest] : withinRadius;
      } else {
        chosen = Object.entries(drivers).filter(([id, d]) => d && matchFilter(d)).map(([id, d]) => ({ id, driver: d, distanceKm: null }));
      }

      tokens = chosen.map(c => c.driver.fcmToken).filter(Boolean);
      if (!tokens.length) {
        return new Response('No online drivers with a token', { status: 200, headers: corsHeaders });
      }

      if (pickup && chosen.length === 1 && chosen[0].distanceKm != null) {
        dispatchWrite = {
          path: `restaurants/${restId}/orders/${orderId}`,
          body: { dispatch: { stage: 'nearest', notifiedIds: chosen.map(c => c.id), notifiedAt: Date.now(), pickupLat: pickup.lat, pickupLng: pickup.lng } },
          queue: { key: 'd_' + restId + '__' + orderId, value: { kind: 'delivery', restId, orderId, notifiedAt: Date.now() } },
        };
      }

      notifTitle = 'New Delivery' + (orderNumber ? ' #' + orderNumber : '') + (restName ? ' — ' + restName : '');

      const bodyLines = [];
      if (itemsSummary) bodyLines.push(itemsSummary);
      if (deliveryFee) bodyLines.push('Delivery fee: ₹' + deliveryFee);
      if (customerAddress) bodyLines.push(customerAddress);
      notifBody = bodyLines.length ? bodyLines.join(' · ') : 'Tap to view delivery details';

      dataPayload = {
        type: 'new_delivery',
        restId,
        orderId,
        restName: restName || '',
        orderNumber: orderNumber || '',
        itemsSummary: itemsSummary || '',
        customerName: customerName || '',
        customerPhone: customerPhone || '',
        customerAddress: customerAddress || '',
        totalAmount: totalAmount != null ? String(totalAmount) : '',
        deliveryFee: deliveryFee != null ? String(deliveryFee) : '',
      };

      // Reuse the existing order-alert channel/sound so no sw.js change
      // is required to hear it.
      channelId = 'order_alerts';
      soundName = 'order_alert';
    } else if (notifType === 'customer') {
      // ---------------- CUSTOMER ORDER-STATUS ALERT ----------------
      // Single-customer notification (order confirmed / delivered / etc.)
      // — triggered from partner.html and driver.html. Looks up the token
      // this one customer saved to customers/{phone}/fcmToken (separate
      // from the "all_customers" topic used for broadcasts).
      const { customerPhone, title, body: msgBody, orderId, restId } = body;
      if (!customerPhone) return new Response('customerPhone required', { status: 400, headers: corsHeaders });

      const token = await rtdbGet(dbUrl, `customers/${customerPhone}/fcmToken`);
      if (!token) {
        return new Response('No FCM token for this customer', { status: 200, headers: corsHeaders });
      }
      tokens = [token];
      notifTitle = title || 'Order update';
      notifBody = msgBody || 'Tap to view your order';
      dataPayload = { type: 'order_status', orderId: orderId || '', restId: restId || '' };
      channelId = 'order_status_alerts';
      soundName = 'default';
    } else if (notifType === 'delivery_ready') {
      // ---------------- PICKUP-READY ALERT (single delivery driver) ----------------
      // Sent by partner.html the moment a shop taps "Mark as ready", but
      // only when that order already has a driver assigned
      // (order.deliveryDriverId) — i.e. the driver accepted it earlier
      // while it was still "preparing" and is presumably already on the
      // way to the shop. Swiggy-style: tells that one driver's phone
      // "it's ready, come collect it now" instead of leaving them to
      // notice the status change on their own. Reuses the 'order_alerts'
      // channel/sound so no native app change is needed.
      const { driverId, restName, orderNumber, orderId, restId } = body;
      if (!driverId) return new Response('driverId required', { status: 400, headers: corsHeaders });

      const token = await rtdbGet(dbUrl, `taxi_drivers/${driverId}/fcmToken`);
      if (!token) {
        return new Response('No FCM token for this driver', { status: 200, headers: corsHeaders });
      }
      tokens = [token];
      notifTitle = 'Pickup Ready!' + (restName ? ' — ' + restName : '');
      notifBody = 'Order' + (orderNumber ? ' #' + orderNumber : '') + ' is ready for pickup. Head over now.';
      dataPayload = { type: 'pickup_ready', restId: restId || '', orderId: orderId || '' };
      // Dedicated no-sound, vibration-only channel (created natively in
      // MainActivity.java — see pickup_ready_alerts in build-apk.yml).
      // Vibration pattern lives on the channel itself; nothing to set here.
      channelId = 'pickup_ready_alerts';
      soundName = null;
    } else if (notifType === 'operator') {
      // ---------------- OPERATOR ALERT (Operator Panel devices) ----------------
      // Sent by the customer app when a NESH Store order lands in the
      // groceryOrderQueue waiting for the operator to pick a shop. Every
      // device that has opened the Operator Panel in the native app saved
      // its FCM token under operator_devices/{key} = { token, updatedAt };
      // all of them get a normal sound notification (works even if the app
      // is closed). No native-app change needed — it reuses the existing
      // 'order_alerts' channel and its custom sound.
      const { title, body: msgBody, orderId } = body;
      await notifyOperatorDevices(env, {
        title: title || 'പുതിയ ഓർഡർ — NESH Store',
        body: msgBody || 'Open the Operator Panel to assign it to a shop',
        orderId: orderId || '',
      });
      return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
    } else if (notifType === 'otp') {
      // ---------------- OTP SMS RELAY (silent, no ring, no tray notification) ----------------
      // Called by the customer app right after it writes a fresh code to
      // otp_requests/{number} in Firebase. Finds whichever single
      // restaurant is currently flagged as the OTP sender and silently
      // pushes { phoneNumber, code } to that restaurant's saved fcmToken
      // (the SAME token field the ordinary order-alert path already uses —
      // no separate device registry needed). That device's AlertFcmService
      // sends the real SMS from its own SIM; nothing is shown on screen.
      const { number, code } = body;
      if (!number || !code) return new Response('number and code required', { status: 400, headers: corsHeaders });

      const senderRes = await fetch(`${dbUrl}/otp_sender_shop.json`);
      const sender = await senderRes.json();
      const senderRestId = sender && sender.restId;
      if (!senderRestId) {
        return new Response('No OTP sender shop is configured in the Operator Panel', { status: 200, headers: corsHeaders });
      }

      const senderToken = await rtdbGet(dbUrl, `restaurants/${senderRestId}/fcmToken`);
      if (!senderToken) {
        return new Response('The configured OTP sender shop has no FCM token yet', { status: 200, headers: corsHeaders });
      }

      tokens = [senderToken];
      notifTitle = null;
      notifBody = null;
      dataPayload = { type: 'send_otp_sms', phoneNumber: String(number), code: String(code) };
      channelId = null;
      soundName = null;
    } else {
      // ---------------- ORDER ALERT (restaurant partner app) ----------------
      const {
        restId,
        orderId,
        orderNumber,
        itemsSummary,
        customerName,
        customerPhone,
        customerAddress,
        totalAmount,
      } = body;
      if (!restId) return new Response('restId required', { status: 400, headers: corsHeaders });

      // 1. Look up the restaurant's saved FCM token from Firebase RTDB.
      const rest = await rtdbFields(dbUrl, `restaurants/${restId}`, ['restaurantName', 'name', 'fcmToken']);
      // Also tell every Operator Panel device (silent notification, no sound).
      // Runs even if this shop has no FCM token; never blocks the shop's own alert.
      await notifyOperatorDevices(env, {
        // Operator format: shop + total first (what the operator scans for), then order #, items, customer.
        title: 'പുതിയ ഓർഡർ — ' + ((rest && (rest.restaurantName || rest.name)) || 'Shop') + (totalAmount ? ' · ₹' + totalAmount : ''),
        body: [orderNumber ? 'Order #' + orderNumber : '', itemsSummary, customerName].filter(Boolean).join(' · ') || 'Open the Operator Panel',
        orderId: orderId || '',
      });
      const token = rest && rest.fcmToken;
      if (!token) {
        return new Response('No FCM token for this restaurant', { status: 200, headers: corsHeaders });
      }
      tokens = [token];

      notifTitle = 'New Order' + (orderNumber ? ' #' + orderNumber : '') +
        (customerName ? ' — ' + customerName : '');

      const bodyLines = [];
      if (itemsSummary) bodyLines.push(itemsSummary);
      if (totalAmount) bodyLines.push('Total: ₹' + totalAmount);
      if (customerPhone) bodyLines.push('Ph: ' + customerPhone);
      if (customerAddress) bodyLines.push(customerAddress);
      notifBody = bodyLines.length ? bodyLines.join(' · ') : 'Tap to view order details';

      dataPayload = {
        type: 'new_order',
        restId,
        orderId: orderId || '',
        orderNumber: orderNumber || '',
        itemsSummary: itemsSummary || '',
        customerName: customerName || '',
        customerPhone: customerPhone || '',
        customerAddress: customerAddress || '',
        totalAmount: totalAmount != null ? String(totalAmount) : '',
      };

      channelId = 'order_alerts';
      soundName = 'order_alert';
    }

    // 2. Get an OAuth access token for both the FCM HTTP v1 API and (when
    //    we have dispatch metadata to save) authenticated RTDB writes,
    //    using the same service account key stored in env.
    const accessToken = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);

    // 3. Send the push to every collected token, flagged for the relevant
    //    Android channel (custom sound) with high priority so the screen
    //    wakes up like an incoming call/alarm, even if the app is fully
    //    closed. FCM HTTP v1 sends one token per call, so fan out with
    //    Promise.all — a single failed token never blocks the others.
    //
    //    Ride alerts, restaurant order alerts, and now driver delivery-job
    //    alerts are all sent as a DATA-ONLY message (no top-level
    //    "notification" key) on purpose: that is the only way Android
    //    guarantees delivery to the app's own
    //    FirebaseMessagingService.onMessageReceived() even when the app
    //    has been fully killed, which is what lets each app show its own
    //    full-screen ringing UI (like an incoming call) instead of a
    //    plain system notification.
    const sendResults = await Promise.all(tokens.map(async (token) => {
      const isSilentData = SILENT_DATA_TYPES.has(dataPayload.type);
      const isFullScreenAlert = FULL_SCREEN_ALERT_TYPES.has(dataPayload.type);
      const message = isSilentData
        ? {
            // No "notification" key at all — nothing is ever shown to the
            // person, but android.priority:'high' still guarantees this
            // reaches the app's FirebaseMessagingService even if the app
            // is fully killed, same as the full-screen alerts below.
            token,
            data: dataPayload,
            android: { priority: 'high' },
          }
        : isFullScreenAlert
        ? {
            token,
            data: Object.assign({}, dataPayload, { title: notifTitle, body: notifBody }),
            android: { priority: 'high' },
          }
        : {
            token,
            notification: { title: notifTitle, body: notifBody },
            data: dataPayload,
            android: {
              priority: 'high',
              notification: { channel_id: channelId, sound: soundName, title: notifTitle, body: notifBody },
            },
          };
      const fcmRes = await fetch(
        `https://fcm.googleapis.com/v1/projects/${projectId}/messages:send`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ message }),
        }
      );
      const result = await fcmRes.json().catch(() => ({}));
      return { ok: fcmRes.ok, result };
    }));

    // Operator alerts: forget any device whose token FCM says is dead.
    if (operatorKeyByToken) {
      await Promise.all(sendResults.map(async (r, i) => {
        const err = r.result && r.result.error;
        const dead = !r.ok && err && (err.status === 'NOT_FOUND' || err.status === 'UNREGISTERED' ||
          (Array.isArray(err.details) && err.details.some(d => d.errorCode === 'UNREGISTERED')));
        const key = dead ? operatorKeyByToken[tokens[i]] : null;
        if (key) {
          await fetch(`${dbUrl}/operator_devices/${key}.json`, {
            method: 'DELETE', headers: { Authorization: `Bearer ${accessToken}` },
          }).catch(() => {});
        }
      }));
    }

    // 4. Save "who got notified, and when" so the Cron Trigger below (or
    //    a future call to this same worker) knows whether this ride/job
    //    is still in its nearest-driver-only window or is due to widen
    //    into a full broadcast. Best-effort — a failed write here just
    //    means escalation timing falls back to the ride's own createdAt.
    if (dispatchWrite) {
      await fetch(`${dbUrl}/${dispatchWrite.path}.json`, {
        method: 'PATCH',
        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(dispatchWrite.body),
      }).catch(() => {});
      if (dispatchWrite.queue) {
        const putQueue = () => fetch(`${dbUrl}/dispatch_queue/${dispatchWrite.queue.key}.json`, {
          method: 'PUT',
          headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(dispatchWrite.queue.value),
        });
        // one retry — if this entry were lost, the nearest->broadcast escalation would not fire
        await putQueue().then((r) => { if (!r.ok) return putQueue(); }).catch(() => putQueue().catch(() => {}));
      }
    }

    const anyOk = sendResults.some(r => r.ok);
    return new Response(JSON.stringify({ sent: sendResults.length, results: sendResults }), {
      status: anyOk ? 200 : 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  },

  // ---------------------------------------------------------------------
  // CRON TRIGGER — runs on the schedule set in the dashboard (DEPLOY step
  // 4 above; every minute is the free-tier minimum). Finds any ride or
  // delivery job that's still sitting unclaimed after being offered to
  // just its nearest driver, and broadcasts it to every other matching
  // online driver within MAX_MATCH_RADIUS_KM — the same "widen the net"
  // behaviour Uber uses when the closest driver doesn't respond in time.
  // ---------------------------------------------------------------------
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runEscalationSweep(env));
    ctx.waitUntil(runLoyaltyReminderSweep(env).catch(() => {}));   // Hero Offer: 'reward expires soon' push
  },
};

// Escalation sweep. BEFORE: every minute it downloaded rides.json + taxi_drivers.json +
// restaurants.json (all shops, menus, order history) even when nothing was waiting.
// NOW: it reads only dispatch_queue (a few tiny entries written when a nearest-driver
// offer goes out). Empty queue -> returns immediately, almost zero bandwidth.
// Escalation rules / timing / messages are unchanged.
async function runEscalationSweep(env) {
  const dbUrl = env.FIREBASE_DB_URL.replace(/\/$/, '');
  const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const projectId = serviceAccount.project_id;
  const now = Date.now();

  const token = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const authH = { Authorization: `Bearer ${token}` };

  const qRes = await fetch(`${dbUrl}/dispatch_queue.json`, { headers: authH });
  if (!qRes.ok) return;
  const queue = (await qRes.json()) || {};
  // cap per run so we stay well under Cloudflare's subrequest limit; the rest go next minute
  const entries = Object.entries(queue).filter(([k, q]) => q && typeof q === 'object').slice(0, 10);
  if (!entries.length) return;

  const dropQueue = (key) => fetch(`${dbUrl}/dispatch_queue/${key}.json`, { method: 'DELETE', headers: authH }).catch(() => {});
  const STALE_MS = 60 * 60 * 1000;   // give up on an entry that never resolved
  const due = [];

  await Promise.all(entries.map(async ([key, q]) => {
    try {
      if (q.notifiedAt && now - q.notifiedAt > STALE_MS) { await dropQueue(key); return; }

      if (q.kind === 'ride' && q.rideId) {
        const ride = await rtdbGet(dbUrl, `rides/${q.rideId}`, token);
        const d = ride && ride.dispatch;
        // ride gone / accepted / cancelled / already broadcast -> nothing left to do
        if (!ride || ride.status !== 'searching' || !d || d.stage !== 'nearest') { await dropQueue(key); return; }
        if (now - (d.notifiedAt || 0) < NEAREST_TIMEOUT_MS) return;
        due.push({ key, kind: 'ride', rideId: q.rideId, ride, d });
      } else if (q.kind === 'delivery' && q.restId && q.orderId) {
        const order = await rtdbGet(dbUrl, `restaurants/${q.restId}/orders/${q.orderId}`, token);
        const d = order && order.dispatch;
        if (!order || order.status !== 'ready' || order.deliveryDriverId || !d || d.stage !== 'nearest') { await dropQueue(key); return; }
        if (now - (d.notifiedAt || 0) < DELIVERY_NEAREST_TIMEOUT_MS) return;
        const rest = await rtdbFields(dbUrl, `restaurants/${q.restId}`, ['goSeries', 'location', 'name'], token);
        if (!rest.goSeries) { await dropQueue(key); return; }
        due.push({ key, kind: 'delivery', restId: q.restId, orderId: q.orderId, order, d, rest });
      } else {
        await dropQueue(key);   // unknown/garbled entry
      }
    } catch (e) { /* try again next minute */ }
  }));

  if (!due.length) return;
  const drivers = await fetchOnlineDrivers(dbUrl);   // only fetched when something is actually due

  const jobs = [];
  due.forEach((x) => {
    const d = x.d;
    if (x.kind === 'ride') {
      const ride = x.ride, rideId = x.rideId;
      const pickup = (typeof d.pickupLat === 'number' && typeof d.pickupLng === 'number') ? { lat: d.pickupLat, lng: d.pickupLng } : (ride.pickup || null);
      if (!pickup) return;
      const matchFilter = dr => dr.isOnline === true && dr.vehicleType === ride.vehicleType && dr.fcmToken && (!dr.services || dr.services.taxi !== false) && !(d.notifiedIds || []).includes(dr._id);
      const ranked = rankDriversByDistance(drivers, pickup, () => true)
        .filter(r => matchFilter(Object.assign({ _id: r.id }, r.driver)))
        .filter(r => r.distanceKm == null || r.distanceKm <= MAX_MATCH_RADIUS_KM);
      if (!ranked.length) return;
      jobs.push({
        key: x.key, kind: 'ride', id: rideId, path: `rides/${rideId}`,
        tokens: ranked.map(r => r.driver.fcmToken),
        notifiedIds: [...(d.notifiedIds || []), ...ranked.map(r => r.id)],
        title: 'New Ride Request' + (ride.customerName ? ' — ' + ride.customerName : ''),
        body: [ride.pickup?.text && ('Pickup: ' + ride.pickup.text), ride.drop?.text && ('Drop: ' + ride.drop.text), ride.fare && ('Fare: ₹' + ride.fare)].filter(Boolean).join(' · ') || 'Tap to view ride details',
        data: { type: 'new_ride', rideId, pickupAddress: ride.pickup?.text || '', dropAddress: ride.drop?.text || '', customerName: ride.customerName || '', customerPhone: ride.customerNumber || '', fare: ride.fare != null ? String(ride.fare) : '', vehicleType: ride.vehicleType || '' },
        channelId: 'ride_alerts', soundName: 'ride_alert',
      });
    } else {
      const { restId, orderId, order, rest } = x;
      const pickup = (typeof d.pickupLat === 'number' && typeof d.pickupLng === 'number') ? { lat: d.pickupLat, lng: d.pickupLng } : parseLatLngStr(rest.location);
      if (!pickup) return;
      const matchFilter = dr => dr.isOnline === true && dr.vehicleType === 'bike' && dr.fcmToken && (!dr.services || dr.services.delivery !== false) && !(d.notifiedIds || []).includes(dr._id);
      const ranked = rankDriversByDistance(drivers, pickup, () => true)
        .filter(r => matchFilter(Object.assign({ _id: r.id }, r.driver)))
        .filter(r => r.distanceKm == null || r.distanceKm <= MAX_MATCH_RADIUS_KM);
      if (!ranked.length) return;
      jobs.push({
        key: x.key, kind: 'delivery', id: `${restId}::${orderId}`, path: `restaurants/${restId}/orders/${orderId}`,
        tokens: ranked.map(r => r.driver.fcmToken),
        notifiedIds: [...(d.notifiedIds || []), ...ranked.map(r => r.id)],
        title: 'New Delivery' + (order.orderNumber ? ' #' + order.orderNumber : '') + (rest.name ? ' — ' + rest.name : ''),
        body: [order.itemsSummary, order.deliveryFee && ('Delivery fee: ₹' + order.deliveryFee), order.customerAddress].filter(Boolean).join(' · ') || 'Tap to view delivery details',
        data: { type: 'new_delivery', restId, orderId, restName: rest.name || '', orderNumber: order.orderNumber || '', itemsSummary: order.itemsSummary || '', customerName: order.customerName || '', customerPhone: order.customerPhone || '', customerAddress: order.customerAddress || '', totalAmount: order.totalAmount != null ? String(order.totalAmount) : '', deliveryFee: order.deliveryFee != null ? String(order.deliveryFee) : '' },
        channelId: 'order_alerts', soundName: 'order_alert',
      });
    }
  });

  if (!jobs.length) return;

  await Promise.all(jobs.map(async (job) => {
    const isFullScreenAlert = FULL_SCREEN_ALERT_TYPES.has(job.data.type);
    const message = isFullScreenAlert
      ? { data: Object.assign({}, job.data, { title: job.title, body: job.body }), android: { priority: 'high' } }
      : {
          notification: { title: job.title, body: job.body },
          data: job.data,
          android: { priority: 'high', notification: { channel_id: job.channelId, sound: job.soundName, title: job.title, body: job.body } },
        };
    await Promise.all(job.tokens.filter(Boolean).map(fcmToken =>
      fetch(`https://fcm.googleapis.com/v1/projects/${projectId}/messages:send`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: Object.assign({ token: fcmToken }, message) }),
      }).catch(() => {})
    ));
    await fetch(`${dbUrl}/${job.path}.json`, {
      method: 'PATCH',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ dispatch: { stage: 'broadcast', notifiedIds: job.notifiedIds, notifiedAt: now } }),
    }).catch(() => {});
    await dropQueue(job.key);   // broadcast done -> nothing more to watch
  }));
}

// Signs a JWT with the service account's private key and exchanges it for
// a short-lived Google OAuth access token. Pure Web Crypto — works fine
// inside Cloudflare Workers (no Node.js APIs needed).
let _tokCache = { key: null, token: null, exp: 0 };
async function getGoogleAccessToken(serviceAccountJson) {
  // Re-use the token for 50 min (Google issues it for 60) instead of signing a
  // new JWT + calling Google on every single request.
  if (_tokCache.token && _tokCache.key === serviceAccountJson && Date.now() < _tokCache.exp) return _tokCache.token;
  const sa = JSON.parse(serviceAccountJson);
  const now = Math.floor(Date.now() / 1000);

  const header = { alg: 'RS256', typ: 'JWT' };
  const claimSet = {
    iss: sa.client_email,
    // messaging (to send pushes) + database (to write dispatch metadata,
    // e.g. rides/{id}.dispatch, so the escalation sweep can read it back)
    // + the general firebase scope (needed for the Instance ID API call
    // that subscribes a token to the "all_customers" topic)
    scope: 'https://www.googleapis.com/auth/firebase.messaging https://www.googleapis.com/auth/firebase.database https://www.googleapis.com/auth/firebase',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600,
  };

  const enc = (obj) =>
    btoa(JSON.stringify(obj)).replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');

  const unsigned = `${enc(header)}.${enc(claimSet)}`;

  const key = await crypto.subtle.importKey(
    'pkcs8',
    pemToArrayBuffer(sa.private_key),
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    key,
    new TextEncoder().encode(unsigned)
  );

  const jwt = `${unsigned}.${arrayBufferToBase64Url(signature)}`;

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`,
  });
  const tokenData = await tokenRes.json();
  if (tokenData.access_token) _tokCache = { key: serviceAccountJson, token: tokenData.access_token, exp: Date.now() + 50 * 60 * 1000 };
  return tokenData.access_token;
}

function pemToArrayBuffer(pem) {
  const b64 = pem.replace(/-----[^-]+-----/g, '').replace(/\s/g, '');
  const raw = atob(b64);
  const buf = new ArrayBuffer(raw.length);
  const view = new Uint8Array(buf);
  for (let i = 0; i < raw.length; i++) view[i] = raw.charCodeAt(i);
  return buf;
}

function arrayBufferToBase64Url(buf) {
  let str = '';
  const bytes = new Uint8Array(buf);
  for (let i = 0; i < bytes.byteLength; i++) str += String.fromCharCode(bytes[i]);
  return btoa(str).replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
}


// ---------------------------------------------------------------------
// HERO OFFER (loyalty reward) — AUTOMATIC "your reward expires soon" PUSH.
// Runs from the same Cron Trigger as the sweep above (cron = every minute,
// this only does work once every 10 minutes). Uses the settings the operator
// saves in partner.html -> Loyalty Reward (app_config/loyalty):
//   discDays / prodDays = days a CLAIMED coupon / free item stays valid
//   remindHours         = send when this many hours are left (blank = 24, 0 = off)
// Customers who claimed are at loyalty/{phone}/claimedAt (cleared by the app
// once the reward is used). expNotified = the claimedAt a reminder was
// already sent for, so each claim gets exactly one push. Token comes from
// customers/{phone}/fcmToken, same as the 'customer' push above.
// ---------------------------------------------------------------------
function loyaltyRewardTextW(v) {
  if (v.rewardType === 'product') return 'FREE ' + (v.productName || 'gift');
  const x = Number(v.value) || 0;
  return v.type === 'percent' ? x + '% OFF' + (Number(v.maxDiscount) > 0 ? ' (up to \u20B9' + v.maxDiscount + ')' : '') : '\u20B9' + x + ' OFF';
}
function loyaltyFmtDateW(ms) {
  return new Date(ms).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' });
}
function loyaltyLeftW(ms) {
  if (ms <= 0) return '0m';
  const m = Math.floor(ms / 60000), d = Math.floor(m / 1440), h = Math.floor((m % 1440) / 60);
  return d ? d + 'd ' + h + 'h' : (h ? h + 'h ' : '') + (m % 60) + 'm';
}
async function runLoyaltyReminderSweep(env) {
  const now = Date.now();
  if (new Date(now).getUTCMinutes() % 10 !== 0) return;   // every 10 minutes is plenty
  const dbUrl = env.FIREBASE_DB_URL.replace(/\/$/, '');
  const projectId = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON).project_id;
  const accessToken = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const auth = { Authorization: `Bearer ${accessToken}` };

  const cfg = await (await fetch(`${dbUrl}/app_config/loyalty.json`, { headers: auth })).json();
  if (!cfg || cfg.on === false) return;
  if (Number(cfg.endAt) > 0 && Number(cfg.endAt) <= now) return;   // whole Hero Offer already ended
  const days = Math.max(0, Number(cfg.rewardType === 'product' ? cfg.prodDays : cfg.discDays) || 0);
  const rem = cfg.remindHours == null ? 24 : Number(cfg.remindHours);
  if (!(days > 0) || !(rem > 0) || days * 24 <= rem) return;   // no expiry, reminder off, or reward shorter than the reminder window

  // only customers who have a claimed reward; falls back to reading the node if Firebase has no index on claimedAt
  let all = null;
  try {
    const qr = await fetch(`${dbUrl}/loyalty.json?orderBy=%22claimedAt%22&startAt=1`, { headers: auth });
    const qj = await qr.json();
    if (qr.ok && !(qj && qj.error)) all = qj || {};
  } catch (e) {}
  if (all === null) all = (await (await fetch(`${dbUrl}/loyalty.json`, { headers: auth })).json()) || {};

  for (const [phone, c] of Object.entries(all)) {
    const at = Number(c && c.claimedAt) || 0;
    if (!at) continue;
    const exp = at + days * 86400000;
    if (exp <= now || exp - now > rem * 3600000 || c.expNotified === at) continue;

    // mark first, so a slow or failed send is never repeated every 10 minutes
    await fetch(`${dbUrl}/loyalty/${phone}/expNotified.json`, {
      method: 'PUT', headers: { ...auth, 'Content-Type': 'application/json' }, body: JSON.stringify(at),
    }).catch(() => {});

    const token = await rtdbGet(dbUrl, `customers/${phone}/fcmToken`, accessToken);
    if (!token) continue;

    const rt = loyaltyRewardTextW(cfg).replace(/ \(up to[^)]*\)/, ''), left = loyaltyLeftW(exp - now), when = loyaltyFmtDateW(exp);
    const title = cfg.rewardType === 'product' ? `\uD83C\uDF81 Your ${rt} is waiting!` : `\u23F3 Your ${rt} reward expires soon!`;
    const body = cfg.rewardType === 'product'
      ? `\u23F3 Only ${left} left \u2014 it expires on ${when}. Order now and grab it before it's gone! \uD83C\uDF89`
      : `Last chance! It ends on ${when} (${left} left). Order now & save before it's gone! \uD83C\uDF89`;
    await fetch(`https://fcm.googleapis.com/v1/projects/${projectId}/messages:send`, {
      method: 'POST',
      headers: { ...auth, 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: {
        token,
        notification: { title, body },
        data: { type: 'order_status', orderId: '', restId: '' },
        android: { priority: 'high', notification: { channel_id: 'order_status_alerts', sound: 'default', title, body } },
      } }),
    }).catch(() => {});
  }
}
