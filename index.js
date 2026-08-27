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
const NEAREST_TIMEOUT_MS = 25 * 1000;
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
const FULL_SCREEN_ALERT_TYPES = new Set(['new_ride', 'new_order', 'new_delivery']);

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

    const notifType = body.type === 'ride' ? 'ride' : (body.type === 'delivery' ? 'delivery' : 'order');

    const dbUrl = env.FIREBASE_DB_URL.replace(/\/$/, '');
    const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
    const projectId = serviceAccount.project_id;

    let tokens, notifTitle, notifBody, dataPayload, channelId, soundName;
    // Filled in for 'ride'/'delivery' so we can write dispatch metadata
    // (who was notified, and when) after the push goes out — the Cron
    // Trigger's `scheduled` handler reads this back to decide when to
    // widen from "nearest driver only" to a full broadcast.
    let dispatchWrite = null; // { path, body }

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
      } = body;
      if (!vehicleType) return new Response('vehicleType required', { status: 400, headers: corsHeaders });

      const driversRes = await fetch(`${dbUrl}/taxi_drivers.json`);
      const drivers = (await driversRes.json()) || {};
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
        };
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

      const [driversRes, restRes] = await Promise.all([
        fetch(`${dbUrl}/taxi_drivers.json`),
        fetch(`${dbUrl}/restaurants/${restId}.json`),
      ]);
      const drivers = (await driversRes.json()) || {};
      const rest = (await restRes.json()) || {};
      const pickup = parseLatLngStr(rest.location);
      const matchFilter = d => d.isOnline === true && d.fcmToken && (!d.services || d.services.delivery !== false);

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
      const restRes = await fetch(`${dbUrl}/restaurants/${restId}.json`);
      const rest = await restRes.json();
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
      const isFullScreenAlert = FULL_SCREEN_ALERT_TYPES.has(dataPayload.type);
      const message = isFullScreenAlert
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
  },
};

async function runEscalationSweep(env) {
  const dbUrl = env.FIREBASE_DB_URL.replace(/\/$/, '');
  const serviceAccount = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const projectId = serviceAccount.project_id;
  const now = Date.now();

  const [ridesRes, driversRes, restaurantsRes] = await Promise.all([
    fetch(`${dbUrl}/rides.json`),
    fetch(`${dbUrl}/taxi_drivers.json`),
    fetch(`${dbUrl}/restaurants.json`),
  ]);
  const rides = (await ridesRes.json()) || {};
  const drivers = (await driversRes.json()) || {};
  const restaurants = (await restaurantsRes.json()) || {};

  let accessToken = null; // fetched lazily, only if there's actually work to do
  const getToken = async () => {
    if (!accessToken) accessToken = await getGoogleAccessToken(env.FIREBASE_SERVICE_ACCOUNT_JSON);
    return accessToken;
  };

  const jobs = [];

  // ---- Rides still "searching" past their nearest-only window ----
  Object.entries(rides).forEach(([rideId, ride]) => {
    if (!ride || ride.status !== 'searching') return;
    const d = ride.dispatch;
    if (!d || d.stage !== 'nearest') return;
    if (now - (d.notifiedAt || 0) < NEAREST_TIMEOUT_MS) return;
    const pickup = (typeof d.pickupLat === 'number' && typeof d.pickupLng === 'number') ? { lat: d.pickupLat, lng: d.pickupLng } : (ride.pickup || null);
    if (!pickup) return;
    const matchFilter = dr => dr.isOnline === true && dr.vehicleType === ride.vehicleType && dr.fcmToken && (!dr.services || dr.services.taxi !== false) && !(d.notifiedIds || []).includes(dr._id);
    const ranked = rankDriversByDistance(drivers, pickup, () => true)
      .filter(r => matchFilter(Object.assign({ _id: r.id }, r.driver)))
      .filter(r => r.distanceKm == null || r.distanceKm <= MAX_MATCH_RADIUS_KM);
    if (!ranked.length) return;
    jobs.push({
      kind: 'ride', id: rideId, path: `rides/${rideId}`,
      tokens: ranked.map(r => r.driver.fcmToken),
      notifiedIds: [...(d.notifiedIds || []), ...ranked.map(r => r.id)],
      title: 'New Ride Request' + (ride.customerName ? ' — ' + ride.customerName : ''),
      body: [ride.pickup?.text && ('Pickup: ' + ride.pickup.text), ride.drop?.text && ('Drop: ' + ride.drop.text), ride.fare && ('Fare: ₹' + ride.fare)].filter(Boolean).join(' · ') || 'Tap to view ride details',
      data: { type: 'new_ride', rideId, pickupAddress: ride.pickup?.text || '', dropAddress: ride.drop?.text || '', customerName: ride.customerName || '', customerPhone: ride.customerNumber || '', fare: ride.fare != null ? String(ride.fare) : '', vehicleType: ride.vehicleType || '' },
      channelId: 'ride_alerts', soundName: 'ride_alert',
    });
  });

  // ---- Delivery jobs still "ready" (unclaimed) past their window ----
  Object.entries(restaurants).forEach(([restId, rest]) => {
    const orders = (rest && rest.orders) || {};
    Object.entries(orders).forEach(([orderId, order]) => {
      if (!order || order.status !== 'ready' || order.deliveryDriverId) return;
      const d = order.dispatch;
      if (!d || d.stage !== 'nearest') return;
      if (now - (d.notifiedAt || 0) < NEAREST_TIMEOUT_MS) return;
      const pickup = (typeof d.pickupLat === 'number' && typeof d.pickupLng === 'number') ? { lat: d.pickupLat, lng: d.pickupLng } : parseLatLngStr(rest.location);
      if (!pickup) return;
      const matchFilter = dr => dr.isOnline === true && dr.fcmToken && (!dr.services || dr.services.delivery !== false) && !(d.notifiedIds || []).includes(dr._id);
      const ranked = rankDriversByDistance(drivers, pickup, () => true)
        .filter(r => matchFilter(Object.assign({ _id: r.id }, r.driver)))
        .filter(r => r.distanceKm == null || r.distanceKm <= MAX_MATCH_RADIUS_KM);
      if (!ranked.length) return;
      jobs.push({
        kind: 'delivery', id: `${restId}::${orderId}`, path: `restaurants/${restId}/orders/${orderId}`,
        tokens: ranked.map(r => r.driver.fcmToken),
        notifiedIds: [...(d.notifiedIds || []), ...ranked.map(r => r.id)],
        title: 'New Delivery' + (order.orderNumber ? ' #' + order.orderNumber : '') + (rest.name ? ' — ' + rest.name : ''),
        body: [order.itemsSummary, order.deliveryFee && ('Delivery fee: ₹' + order.deliveryFee), order.customerAddress].filter(Boolean).join(' · ') || 'Tap to view delivery details',
        data: { type: 'new_delivery', restId, orderId, restName: rest.name || '', orderNumber: order.orderNumber || '', itemsSummary: order.itemsSummary || '', customerName: order.customerName || '', customerPhone: order.customerPhone || '', customerAddress: order.customerAddress || '', totalAmount: order.totalAmount != null ? String(order.totalAmount) : '', deliveryFee: order.deliveryFee != null ? String(order.deliveryFee) : '' },
        channelId: 'order_alerts', soundName: 'order_alert',
      });
    });
  });

  if (!jobs.length) return;
  const token = await getToken();

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
  }));
}

// Signs a JWT with the service account's private key and exchanges it for
// a short-lived Google OAuth access token. Pure Web Crypto — works fine
// inside Cloudflare Workers (no Node.js APIs needed).
async function getGoogleAccessToken(serviceAccountJson) {
  const sa = JSON.parse(serviceAccountJson);
  const now = Math.floor(Date.now() / 1000);

  const header = { alg: 'RS256', typ: 'JWT' };
  const claimSet = {
    iss: sa.client_email,
    // messaging (to send pushes) + database (to write dispatch metadata,
    // e.g. rides/{id}.dispatch, so the escalation sweep can read it back)
    scope: 'https://www.googleapis.com/auth/firebase.messaging https://www.googleapis.com/auth/firebase.database',
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
