# Nexova Backoffice Dashboard

This package is the static operations overview built with Next.js App Router,
React, and TypeScript. It is separate from the API-connected incident manager.

## Scripts

- `npm install`
- `npm run dev`
- `npm run typecheck`
- `npm run build`
- `npm run start`

The development and production servers use port `3001`.

The `/reporting` page reads the latest published week from `GET /reporting/kpis` using a server-side request. `REPORTING_API_URL` sets the reporting service base URL and defaults to `http://127.0.0.1:8005` for local development. The page forwards the existing HttpOnly `nexova_session` token only as a server-side Bearer header; the reporting service validates it with the Auth API. Do not expose session tokens or service credentials to browser code.
