# BULK Terminal Backend

Backend API for BULK Terminal Community Dashboard.

## Features

- 🔐 **Auth** - Email/password registration & login with JWT
- 🏆 **Leaderboards** - Top PnL, Most Liquidated, Whales, Most Active
- 📊 **Analytics** - OI history, funding rates, volume, long/short ratio
- 👛 **Wallet Tracking** - Track any wallet, view history
- 📋 **Watchlist** - Save wallets to your personal watchlist
- 🔄 **Data Collector** - Automatic data collection from BULK API

## Tech Stack

- **Runtime**: Node.js + TypeScript
- **Framework**: Express
- **Database**: PostgreSQL
- **Auth**: JWT (jsonwebtoken)
- **Cron**: node-cron

## Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Set Up Database (Railway)

1. Go to [railway.app](https://railway.app)
2. Create new project → Add PostgreSQL
3. Copy the `DATABASE_URL` from the PostgreSQL service

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env`:
```
DATABASE_URL=postgresql://postgres:xxx@xxx.railway.app:5432/railway
JWT_SECRET=generate-a-random-secret-here
FRONTEND_URL=https://your-frontend.vercel.app
```

### 4. Run Locally

```bash
# Development (with hot reload)
npm run dev

# Production build
npm run build
npm start
```

## Deploy to Railway

### Option 1: GitHub Deploy

1. Push this folder to GitHub
2. In Railway: New Project → Deploy from GitHub
3. Select your repo
4. Add environment variables in Railway dashboard
5. Deploy!

### Option 2: Railway CLI

```bash
npm install -g @railway/cli
railway login
railway init
railway up
```

## API Endpoints

### Auth
```
POST /api/auth/register  - Register new user
POST /api/auth/login     - Login
GET  /api/auth/me        - Get current user (requires auth)
```

### Leaderboards
```
GET /api/leaderboard/pnl?timeframe=24h|7d|30d|all
GET /api/leaderboard/liquidated?timeframe=...
GET /api/leaderboard/whales
GET /api/leaderboard/active?timeframe=...
GET /api/leaderboard/volume?timeframe=...
GET /api/leaderboard/liquidations/recent
GET /api/leaderboard/trades/recent
```

### Analytics
```
GET /api/analytics/open-interest/:symbol?hours=168
GET /api/analytics/funding-rate/:symbol?hours=168
GET /api/analytics/volume/:symbol?hours=168
GET /api/analytics/long-short-ratio/:symbol?hours=168
GET /api/analytics/liquidation-heatmap/:symbol
GET /api/analytics/correlation
GET /api/analytics/exchange-health
```

### Wallet
```
GET  /api/wallet/:address           - Get wallet data
POST /api/wallet/:address/track     - Start tracking wallet
GET  /api/wallet/:address/trades    - Get trade history
GET  /api/wallet/:address/liquidations

# Requires auth:
GET    /api/wallet/user/watchlist
POST   /api/wallet/watchlist/:address
DELETE /api/wallet/watchlist/:address
```

## Data Collection

The backend automatically collects data:

- **Every 1 minute**: Market stats (price, OI, volume, funding)
- **Every 5 minutes**: Trader snapshots (PnL, positions)
- **Daily at 3am**: Cleanup old data (>30 days)

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `JWT_SECRET` | Secret for JWT signing |
| `FRONTEND_URL` | Frontend URL for CORS |
| `PORT` | Server port (default: 3001) |
| `NODE_ENV` | Environment (development/production) |

## License

MIT
