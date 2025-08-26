// credentials.js
const API_BASE = "http://127.0.0.1:8000";

async function saveCredential(service, key, value) {
  const token = localStorage.getItem("token");
  const res = await fetch(`${API_BASE}/api/tenant/credentials`, {
    method: "POST",
    headers: {
      "Content-Type":"application/json",
      "Authorization": `Bearer ${token}`
    },
    body: JSON.stringify({ service, key, value })
  });
  return res.json();
  
}
