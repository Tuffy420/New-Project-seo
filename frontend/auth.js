// auth.js
const API_BASE = "http://127.0.0.1:8000";

function saveAuth({ access_token, tenant_id }) {
  localStorage.setItem("token", access_token);
  localStorage.setItem("tenant_id", tenant_id);
}
function getToken() { return localStorage.getItem("token"); }
function logout() { localStorage.clear(); location.href = "login.html"; }
function protect() { if (!getToken()) location.href = "login.html"; }

async function register(email, password) {
  const res = await fetch(`${API_BASE}/api/auth/register`, {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ email, password })
  });
  return res.json();
}
async function login(email, password) {
  try {
    const res = await fetch(`${API_BASE}/api/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password })
    });

    const data = await res.json();
    console.log("Server responded with:", res.status, data);

    if (res.ok && data.access_token) saveAuth(data);
    return data;
  } catch (error) {
    console.error("Fetch error:", error);
    return { detail: "Network error" };
  }
}
