import "server-only";

import { cookies } from "next/headers";

export const SESSION_COOKIE = "nexova_session";

export type AuthSession = {
  userId: string;
  sessionId: string;
  expiresAt: string;
};

export function authRequest(path: "/login" | "/logout", init: RequestInit) {
  const apiUrl = process.env.AUTH_API_URL ?? "http://127.0.0.1:8003";
  return fetch(`${apiUrl.replace(/\/$/, "")}${path}`, {
    ...init,
    method: "POST",
    cache: "no-store",
    signal: AbortSignal.timeout(5000),
  });
}

export async function getSessionToken(): Promise<string | null> {
  return (await cookies()).get(SESSION_COOKIE)?.value ?? null;
}

export async function getSession(): Promise<AuthSession | null> {
  const token = await getSessionToken();
  if (!token) return null;

  try {
    const response = await authRequest("/login", {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!response.ok) return null;
    return (await response.json()) as AuthSession;
  } catch {
    return null;
  }
}
