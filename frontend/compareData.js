// compareData.js
const API_BASE = "http://localhost:8000";

// Get token
function getToken() {
  return localStorage.getItem("token");
}

// Redirect to login if no token
if (!getToken()) {
  window.location.href = "login.html";
}

async function compareData(platform) {
  const start1 = document.getElementById("start1").value;
  const end1 = document.getElementById("end1").value;
  const start2 = document.getElementById("start2").value;
  const end2 = document.getElementById("end2").value;

  if (!start1 || !end1 || !start2 || !end2) {
    alert("Please select both date ranges.");
    return;
  }

  try {
    const url = `${API_BASE}/compare/${platform}`;
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + getToken(),
      },
      body: JSON.stringify({ start1, end1, start2, end2 }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    renderResults(platform, data);
  } catch (error) {
    document.getElementById("result-container").innerHTML =
      `<p style="color:red;">Error fetching comparison: ${error.message}</p>`;
  }
}

// ---------- Render Results ----------
function renderResults(platform, data) {
  const container = document.getElementById("result-container");
  // const compBody = document.getElementById("comparison-body");
  // compBody.innerHTML = ""; // clear day-by-day table
  container.innerHTML = ""; // clear previous summary

  if (!data || Object.keys(data).length === 0) {
    container.innerHTML = "<p>No data available for comparison.</p>";
    return;
  }

  let html = `<h3>${platform.toUpperCase()} Comparison</h3>`;

  // ---------------- CLOUDLFARE ----------------
  if (platform.toLowerCase() === "cloudflare") {
    html += `
      <table border="1" cellpadding="5" cellspacing="0">
        <thead>
          <tr>
            <th>Range</th>
            <th>Start</th>
            <th>End</th>
            <th>Total Page Views</th>
            <th>Total Visits</th>
          </tr>
        </thead>
        <tbody>
    `;
    for (const [rangeName, rangeData] of Object.entries(data)) {
      html += `
        <tr>
          <td>${rangeName}</td>
          <td>${rangeData.start ?? "-"}</td>
          <td>${rangeData.end ?? "-"}</td>
          <td>${rangeData.total_page_views ?? "-"}</td>
          <td>${rangeData.total_visits ?? "-"}</td>
        </tr>
      `;
    }
    html += `</tbody></table>`;
  }
// ---------------- GSC ----------------
else if (platform.toLowerCase() === "gsc") {
  for (const [tableName, tableData] of Object.entries(data.comparison || data)) {
    // Format title (remove gsc_ and _daily)
    const tableTitle = tableName.replace(/^gsc_|_daily$/g, "").toUpperCase();
    html += `<h4 style="margin-top: 20px; margin-bottom: 10px;">${tableTitle}</h4>`;

    // Safely extract sample data keys (exclude "date" column)
    const sampleData = tableData.range1?.data?.[0] || {};
    let keys = Object.keys(sampleData).filter(k => k.toLowerCase() !== "date");

    // Build table header
    html += `<table border="1" cellspacing="0" cellpadding="5" style="width: 100%; border-collapse: collapse;">`;
    html += `<thead><tr>`;
    html += `<th>S.NO</th>`;
    html += `<th>DATE RANGE</th>`;
    keys.forEach((key) => {
      html += `<th>${key.replace(/_/g, " ").toUpperCase()}</th>`;
    });
    html += `</tr></thead><tbody>`;

    // Loop rows
    const rows = Math.max(tableData.range1?.data?.length || 0, tableData.range2?.data?.length || 0);
    for (let i = 0; i < rows; i++) {
      const range1Row = tableData.range1?.data?.[i] || {};
      const range2Row = tableData.range2?.data?.[i] || {};

      // Serial number
      html += `<tr><td rowspan="3" style="vertical-align: middle; text-align:center;">${i + 1}</td>`;

      // Range1
      html += `<td>${tableData.range1?.start ?? "-"} – ${tableData.range1?.end ?? "-"}</td>`;
      keys.forEach((key) => {
        html += `<td>${range1Row[key] ?? "-"}</td>`;
      });
      html += `</tr>`;

      // Range2
      html += `<tr>`;
      html += `<td>${tableData.range2?.start ?? "-"} – ${tableData.range2?.end ?? "-"}</td>`;
      keys.forEach((key) => {
        html += `<td>${range2Row[key] ?? "-"}</td>`;
      });
      html += `</tr>`;

      // % Change row ✅ FIXED
      html += `<tr>`;
      html += `<td>% Change</td>`;
      keys.forEach((key) => {
        const val = tableData.percentage_changes?.[key]; // <-- FIXED: access directly by key
        if (val != null && !isNaN(val)) {
          const color = val >= 0 ? "green" : "red";
          html += `<td style="color:${color}; font-weight:bold;">${val.toFixed(2)}%</td>`;
        } else {
          html += `<td>-</td>`;
        }
      });
      html += `</tr>`;
    }

    html += `</tbody></table>`;
  }
}

// ---------------- GA4 ----------------

else if (platform.toLowerCase() === "ga4") {
  for (const [tableName, tableData] of Object.entries(data)) {
    const tableTitle = tableName.replace(/^ga4_|_daily$/g, "").toUpperCase();
    html += `<h4>${tableTitle} Data</h4>`;

    // Detect dimension key
    let dimensionKey = "page_path";
    if (tableName.includes("traffic_acquisition")) dimensionKey = "source_medium";
    if (tableName.includes("country_metrics")) dimensionKey = "country";
    if (tableName.includes("browser_metrics")) dimensionKey = "browser";

    // Collect metric keys (exclude date + dimension)
    const sample = tableData.range1?.data?.[0] || tableData.range2?.data?.[0] || {};
    const metricKeys = Object.keys(sample).filter(
      k => !["start", "end", "date", dimensionKey].includes(k)
    );

    if (metricKeys.length === 0) {
      html += `<p>No data available.</p>`;
      continue;
    }

    html += `
      <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width:100%;">
        <thead style="background:#f5f5f5;">
          <tr>
            <th>S.NO</th>
            <th>Date Range</th>
            <th>${dimensionKey.replace(/_/g," ").toUpperCase()}</th>
            ${metricKeys.map(k => `<th>${k.replace(/_/g," ").toUpperCase()}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
    `;

    // Collect all dimension keys
    const allKeys = Array.from(new Set([
      ...(tableData.range1?.data?.map(r => r[dimensionKey]) || []),
      ...(tableData.range2?.data?.map(r => r[dimensionKey]) || [])
    ]));

    let sno = 1;

    allKeys.forEach(key => {
      const r1Rows = (tableData.range1?.data || []).filter(r => r[dimensionKey] === key);
      const r2Rows = (tableData.range2?.data || []).filter(r => r[dimensionKey] === key);

      // Aggregate values (sum for counts, avg for rates/times if needed)
      const aggValues = (rows, k) => {
        const vals = rows.map(r => parseFloat(r[k]) || 0);
        if (!vals.length) return 0;
        return ["avg_engagement_time","avg_engagement_rate","avg_views_per_user","avg_bounce_rate"].includes(k)
          ? vals.reduce((a,b)=>a+b,0) / vals.length
          : vals.reduce((a,b)=>a+b,0);
      };

      const r1Agg = Object.fromEntries(metricKeys.map(k => [k, aggValues(r1Rows, k)]));
      const r2Agg = Object.fromEntries(metricKeys.map(k => [k, aggValues(r2Rows, k)]));

      // Row for range 1
      html += `
        <tr>
          <td rowspan="3">${sno}</td>
          <td>${tableData.range1?.start} → ${tableData.range1?.end}</td>
          <td>${key ?? "-"}</td>
          ${metricKeys.map(k => `<td>${r1Agg[k] || "-"}</td>`).join("")}
        </tr>
      `;

      // Row for range 2
      html += `
        <tr>
          <td>${tableData.range2?.start} → ${tableData.range2?.end}</td>
          <td>${key ?? "-"}</td>
          ${metricKeys.map(k => `<td>${r2Agg[k] || "-"}</td>`).join("")}
        </tr>
      `;

      // % Change row
      html += `
        <tr style="background:#eef6ff;">
          <td>% Change</td>
          <td></td>
          ${metricKeys.map(k => {
            const v1 = r1Agg[k] || 0;
            const v2 = r2Agg[k] || 0;
            if (!v1 && !v2) return "<td>-</td>";
            const change = v2 && v1 ? ((v2 - v1) / v1) * 100 : (v2 ? 100 : -100);
            const sign = change > 0 ? "+" : "";
            const color = change > 0 ? "green" : (change < 0 ? "red" : "black");
            return `<td style="color:${color};">${sign}${change.toFixed(2)}%</td>`;
          }).join("")}
        </tr>
      `;

      sno++;
    });

    html += `</tbody></table>`;
  }
}

  container.innerHTML = html;
}
