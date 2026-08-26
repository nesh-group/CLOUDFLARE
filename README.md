# NESH Worker — Cloudflare Worker for Order & Ride Alerts

This Cloudflare Worker sends order, ride, and delivery push notifications using Firebase Cloud Messaging (FCM).

## Features

- **Order Alerts** — Restaurant notifications when customer places order
- **Ride Alerts** — Driver notifications with nearest-driver-first dispatch
- **Delivery Alerts** — Driver notifications for delivery jobs
- **Auto-escalation** — Broadcast to all drivers if no one accepts within timeout

## Setup

### 1. Environment Variables

Set these in Cloudflare Worker Settings → Variables:

- `FIREBASE_DB_URL` — Your Firebase Realtime Database URL (e.g., `https://yourproject.firebaseio.com`)
- `FIREBASE_SERVICE_ACCOUNT_JSON` — Your Firebase service account JSON (mark as "Encrypt")

### 2. Cron Trigger

Enable automatic escalation in Worker Settings → Triggers:

- Add Cron Trigger: `* * * * *` (every minute)

### 3. Deploy

```bash
npm install
npm run deploy
```

Or manually in Cloudflare Dashboard:
- Workers & Pages → Create Worker
- Paste `src/index.js` code
- Deploy

Your worker URL will be: `https://your-project-name.workers.dev`

## API Usage

### Order Alert
```json
POST /
{
  "restId": "restaurant-id",
  "orderNumber": "12345",
  "itemsSummary": "2x Biryani, 1x Coke",
  "customerName": "John",
  "customerPhone": "+919999999999",
  "customerAddress": "123 Main St",
  "totalAmount": 500
}
```

### Ride Alert
```json
POST /
{
  "type": "ride",
  "rideId": "ride-123",
  "pickupLat": 12.9716,
  "pickupLng": 77.5946,
  "pickupAddress": "Pickup Location",
  "dropAddress": "Drop Location",
  "customerName": "John",
  "customerPhone": "+919999999999",
  "fare": 250,
  "vehicleType": "auto"
}
```

### Delivery Alert
```json
POST /
{
  "type": "delivery",
  "restId": "restaurant-id",
  "orderId": "order-123",
  "restName": "Restaurant Name",
  "orderNumber": "12345",
  "itemsSummary": "2x Biryani",
  "customerName": "John",
  "customerPhone": "+919999999999",
  "customerAddress": "123 Main St",
  "totalAmount": 500,
  "deliveryFee": 50
}
```

## GitHub → Cloudflare Auto-Deploy

1. Connect this repo to Cloudflare Workers & Pages
2. Push to GitHub → Automatic deploy to `your-worker.workers.dev`

## More Info

See inline comments in `src/index.js` for detailed documentation.
