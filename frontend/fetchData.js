// fetchData.js
function getToken() {
  return localStorage.getItem("token");
}

function logout() {
  localStorage.clear();
  location.href = "login.html";
}

function protect() {
  if (!getToken()) location.href = "login.html";
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function calculateRange(range) {
  const today = new Date();
  let start = new Date(today);
  let end = new Date(today);

  if (range === "today") {
    // do nothing
  } else if (!isNaN(range)) {
    start.setDate(today.getDate() - (parseInt(range) - 1));
  }

  return { start: formatDate(start), end: formatDate(end) };
}

function getPlatformRange(platform) {
  const selected = document.getElementById(`${platform}-range`).value;
  if (selected === "custom") {
    return {
      start: document.getElementById(`${platform}-start`).value,
      end: document.getElementById(`${platform}-end`).value
    };
  }

  let { start, end } = calculateRange(selected);

  if (platform === "gsc") {
    const s = new Date(start);
    const e = new Date(end);
    s.setDate(s.getDate() - 3);
    e.setDate(e.getDate() - 3);
    start = formatDate(s);
    end = formatDate(e);
  }

  return { start, end };
}

async function postWithToken(endpoint, body = {}) {
  const res = await fetch(`${API_BASE}${endpoint}`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${getToken()}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await res.json();
  return res.ok ? data : (data.detail || "Failed");
}

document.addEventListener("DOMContentLoaded", () => {
  protect();

  const platforms = ["gsc", "ga4", "cf"];

  platforms.forEach(platform => {
    const select = document.getElementById(`${platform}-range`);
    const customDiv = document.getElementById(`${platform}-custom`);
    if (select && customDiv) {
      select.addEventListener("change", () => {
        customDiv.style.display = select.value === "custom" ? "block" : "none";
      });
    }
  });

  document.getElementById("fetchForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const msg = document.getElementById("msg");
    msg.innerHTML = "Fetching...";

    const tasks = [];

    platforms.forEach(p => {
      const range = getPlatformRange(p);
      if (range.start && range.end && document.getElementById(`${p}-range`).value !== "none") {
        const endpoint = `/fetch/${p === 'cf' ? 'cloudflare' : p}`;
        tasks.push(
          postWithToken(endpoint, {
            start_date: range.start,
            end_date: range.end
          }).then(m => {
            // handle object display
            const msgStr = typeof m === "object" ? JSON.stringify(m, null, 2) : m;
            return `${p.toUpperCase()}: <pre>${msgStr}</pre>`;
          })
        );
      }
    });

    if (!tasks.length) {
      msg.innerHTML = "⚠️ Please select at least one valid platform with a date range.";
      return;
    }

    const results = await Promise.all(tasks);
    msg.innerHTML = results.join('');
  });

  document.getElementById("logout").onclick = () => logout();
  document.getElementById("goCreds").onclick = () => location.href = 'credentials.html';
});

