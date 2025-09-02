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

  let labels = ["Page Views", "Visits"];
  let range1Values = [];
  let range2Values = [];

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

    if (rangeName === "range1") {
      range1Values = [rangeData.total_page_views || 0, rangeData.total_visits || 0];
    }
    if (rangeName === "range2") {
      range2Values = [rangeData.total_page_views || 0, rangeData.total_visits || 0];
    }
  }

  html += `</tbody></table>`;

  // --- PIE CHARTS ---
  html += `
  <div style="display:flex; gap:30px; margin-top:20px; align-items:flex-start;">
    <div style="text-align:center;">
      <canvas id="cloudflareChart1" width="200" height="200"></canvas>
    </div>
    <div style="text-align:center;">
      <canvas id="cloudflareChart2" width="200" height="200"></canvas>
    </div>
  </div>
  `;

  // --- LINE CHARTS ---
  // --- COMPARISON LINE CHART (Range1 vs Range2) ---
html += `
  <div style="margin-top:40px; text-align:center;">
    <h4>Line Graph (Range 1 vs Range 2)</h4>
    <canvas id="cloudflareLineComparison" height="200"></canvas>
  </div>
`;

  container.innerHTML = html;

  // ----------- PIE CHARTS -----------
  const ctx1 = document.getElementById("cloudflareChart1").getContext("2d");
  new Chart(ctx1, {
    type: "pie",
    data: {
      labels: labels,
      datasets: [{
        data: range1Values,
        backgroundColor: ["#36A2EB", "#FF6384"]
      }]
    },
    options: {
      responsive: true,
      plugins: {
        title: { display: true, text: "Range 1 (Page Views vs Visits)" }
      }
    }
  });

  const ctx2 = document.getElementById("cloudflareChart2").getContext("2d");
  new Chart(ctx2, {
    type: "pie",
    data: {
      labels: labels,
      datasets: [{
        data: range2Values,
        backgroundColor: ["#36A2EB", "#FF6384"]
      }]
    },
    options: {
      responsive: true,
      plugins: {
        title: { display: true, text: "Range 2 (Page Views vs Visits)" }
      }
    }
  });

  // ----------- LINE COMPARISON CHART -----------
const lineLabels = (data.range1?.data || []).map(r => r.date); // assume same dates for both
const lineValues1 = (data.range1?.data || []).map(r => r.total_page_views || 0);
const lineVisits1 = (data.range1?.data || []).map(r => r.total_visits || 0);
const lineValues2 = (data.range2?.data || []).map(r => r.total_page_views || 0);
const lineVisits2 = (data.range2?.data || []).map(r => r.total_visits || 0);

const ctxLineComparison = document.getElementById("cloudflareLineComparison").getContext("2d");
new Chart(ctxLineComparison, {
  type: "line",
  data: {
    labels: lineLabels,
    datasets: [
      {
        label: "Range 1 - Page Views",
        data: lineValues1,
        borderColor: "#36A2EB",
        backgroundColor: "rgba(54,162,235,0.2)",
        fill: true,
        tension: 0.3
      },
      {
        label: "Range 1 - Visits",
        data: lineVisits1,
        borderColor: "#FF6384",
        backgroundColor: "rgba(255,99,132,0.2)",
        fill: true,
        tension: 0.3
      },
      {
        label: "Range 2 - Page Views",
        data: lineValues2,
        borderColor: "#4BC0C0",
        backgroundColor: "rgba(75,192,192,0.2)",
        fill: true,
        tension: 0.3
      },
      {
        label: "Range 2 - Visits",
        data: lineVisits2,
        borderColor: "#9966FF",
        backgroundColor: "rgba(153,102,255,0.2)",
        fill: true,
        tension: 0.3
      }
    ]
  },
  options: {
    responsive: true,
    plugins: {
      title: {
        display: true,
        text: "Range 1 vs Range 2 (Daily Trends)"
      }
    }
  }
});

  return; // stop further processing
}



// ---------------- GSC ----------------
else if (platform.toLowerCase() === "gsc") {
  for (const [tableName, tableData] of Object.entries(data.comparison || data)) {
    const tableTitle = tableName.replace(/^gsc_|_daily$/g, "").toUpperCase();
    html += `<h4 style="margin-top: 20px; margin-bottom: 10px;">${tableTitle}</h4>`;

    // Extract keys (exclude "date")
    const sampleData = tableData.range1?.data?.[0] || {};
    let keys = Object.keys(sampleData).filter(k => k.toLowerCase() !== "date");

    // ✅ Add chart containers
    const chartId1 = `${platform}_${tableName}_range1_chart`;
    const chartId2 = `${platform}_${tableName}_range2_chart`;
    html += `
      <div style="display:flex; gap:20px; margin-bottom:20px;">
        <div style="flex:1;">
          <canvas id="${chartId1}" width="220" height="220"></canvas>
          <p style="text-align:center; font-size:13px;">
            ${tableData.range1?.start ?? "-"} – ${tableData.range1?.end ?? "-"}
          </p>
        </div>
        <div style="flex:1;">
          <canvas id="${chartId2}" width="220" height="220"></canvas>
          <p style="text-align:center; font-size:13px;">
            ${tableData.range2?.start ?? "-"} – ${tableData.range2?.end ?? "-"}
          </p>
        </div>
      </div>
    `;

    // Build table
    html += `<table border="1" cellspacing="0" cellpadding="5" style="width:100%; border-collapse:collapse;">`;
    html += `<thead><tr><th>S.NO</th><th>DATE RANGE</th>`;
    keys.forEach((key) => html += `<th>${key.replace(/_/g," ").toUpperCase()}</th>`);
    html += `</tr></thead><tbody>`;

    const rows = Math.max(tableData.range1?.data?.length || 0, tableData.range2?.data?.length || 0);
    for (let i = 0; i < rows; i++) {
      const r1 = tableData.range1?.data?.[i] || {};
      const r2 = tableData.range2?.data?.[i] || {};

      html += `<tr><td rowspan="3" style="text-align:center; vertical-align:middle;">${i+1}</td>`;

      // Range 1
      html += `<td>${tableData.range1?.start ?? "-"} – ${tableData.range1?.end ?? "-"}</td>`;
      keys.forEach(k => html += `<td>${r1[k] ?? "-"}</td>`);
      html += `</tr>`;

      // Range 2
      html += `<tr><td>${tableData.range2?.start ?? "-"} – ${tableData.range2?.end ?? "-"}</td>`;
      keys.forEach(k => html += `<td>${r2[k] ?? "-"}</td>`);
      html += `</tr>`;

      // % Change
      html += `<tr><td>% Change</td>`;
      keys.forEach(k => {
        const v1 = r1[k] || 0, v2 = r2[k] || 0;
        let change = (v1 && v2) ? ((v2 - v1) / v1) * 100 : (v2 ? 100 : (v1 ? -100 : 0));
        if (isNaN(change)) change = 0;
        const color = change > 0 ? "green" : (change < 0 ? "red" : "black");
        html += `<td style="color:${color}; font-weight:bold;">${change.toFixed(2)}%</td>`;
      });
      html += `</tr>`;
    }
    html += `</tbody></table>`;

    // ✅ Generate Pie Charts (all columns in ONE pie chart per range)
    setTimeout(() => {
      const ctx1 = document.getElementById(chartId1);
      const ctx2 = document.getElementById(chartId2);

      if (ctx1 && tableData.range1?.data?.length) {
        const totals1 = keys.map(k => tableData.range1.data.reduce((s,row)=>s+(row[k]||0),0));
        new Chart(ctx1, {
          type: "pie",
          data: {
            labels: keys,
            datasets: [{
              data: totals1,
              backgroundColor: ["#FF6384","#36A2EB","#FFCE56","#4BC0C0","#9966FF","#FF9F40","#8BC34A"]
            }]
          },
          options: { plugins:{ title:{ display:true, text:`${tableTitle} - Range 1` }, legend:{ position:"bottom" } } }
        });
      }

      if (ctx2 && tableData.range2?.data?.length) {
        const totals2 = keys.map(k => tableData.range2.data.reduce((s,row)=>s+(row[k]||0),0));
        new Chart(ctx2, {
          type: "pie",
          data: {
            labels: keys,
            datasets: [{
              data: totals2,
              backgroundColor: ["#FF6384","#36A2EB","#FFCE56","#4BC0C0","#9966FF","#FF9F40","#8BC34A"]
            }]
          },
          options: { plugins:{ title:{ display:true, text:`${tableTitle} - Range 2` }, legend:{ position:"bottom" } } }
        });
      }
    }, 300);
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

    // Collect metric keys
    const sample = tableData.range1?.data?.[0] || tableData.range2?.data?.[0] || {};
    const metricKeys = Object.keys(sample).filter(
      k => !["start", "end", "date", dimensionKey].includes(k)
    );

    if (metricKeys.length === 0) {
      html += `<p>No data available.</p>`;
      continue;
    }

    // Build Table
    html += `
      <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width:100%; margin-bottom:20px;">
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

    const allKeys = Array.from(new Set([
      ...(tableData.range1?.data?.map(r => r[dimensionKey]) || []),
      ...(tableData.range2?.data?.map(r => r[dimensionKey]) || [])
    ]));

    let sno = 1;

    // Chart data for Range1 & Range2
    const chartLabels = metricKeys.map(k => k.replace(/_/g," ").toUpperCase());
    const chartValues1 = new Array(metricKeys.length).fill(0);
    const chartValues2 = new Array(metricKeys.length).fill(0);

    allKeys.forEach(key => {
      const r1Rows = (tableData.range1?.data || []).filter(r => r[dimensionKey] === key);
      const r2Rows = (tableData.range2?.data || []).filter(r => r[dimensionKey] === key);

      const aggValues = (rows, k) => {
        const vals = rows.map(r => parseFloat(r[k]) || 0);
        if (!vals.length) return 0;
        return ["avg_engagement_time","avg_engagement_rate","avg_views_per_user","avg_bounce_rate"].includes(k)
          ? vals.reduce((a,b)=>a+b,0) / vals.length
          : vals.reduce((a,b)=>a+b,0);
      };

      const r1Agg = Object.fromEntries(metricKeys.map(k => [k, aggValues(r1Rows, k)]));
      const r2Agg = Object.fromEntries(metricKeys.map(k => [k, aggValues(r2Rows, k)]));

      // Aggregate values for pie charts (sum across all rows)
      metricKeys.forEach((k, idx) => {
        chartValues1[idx] += r1Agg[k] || 0;
        chartValues2[idx] += r2Agg[k] || 0;
      });

      // Table rows
      html += `
        <tr>
          <td rowspan="3">${sno}</td>
          <td>${tableData.range1?.start} → ${tableData.range1?.end}</td>
          <td>${key ?? "-"}</td>
          ${metricKeys.map(k => `<td>${r1Agg[k] || "-"}</td>`).join("")}
        </tr>
        <tr>
          <td>${tableData.range2?.start} → ${tableData.range2?.end}</td>
          <td>${key ?? "-"}</td>
          ${metricKeys.map(k => `<td>${r2Agg[k] || "-"}</td>`).join("")}
        </tr>
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

    // Chart containers
    const chartId1 = `${tableName}-chart-range1`;
    const chartId2 = `${tableName}-chart-range2`;
    html += `
      <div style="display:flex; gap:30px; margin-bottom:40px;">
        <div style="flex:1;">
          <h5>Range 1</h5>
          <canvas id="${chartId1}" style="max-height:300px;"></canvas>
        </div>
        <div style="flex:1;">
          <h5>Range 2</h5>
          <canvas id="${chartId2}" style="max-height:300px;"></canvas>
        </div>
      </div>
    `;

    // Render pie charts
    setTimeout(() => {
      const ctx1 = document.getElementById(chartId1)?.getContext("2d");
      const ctx2 = document.getElementById(chartId2)?.getContext("2d");

      if (ctx1) {
        new Chart(ctx1, {
          type: "pie",
          data: {
            labels: chartLabels,
            datasets: [{
              label: "Range 1",
              data: chartValues1,
              backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40", "#8BC34A", "#E91E63"]
            }]
          },
          options: { plugins: { title: { display: true, text: `${tableTitle} - Range 1` } } }
        });
      }

      if (ctx2) {
        new Chart(ctx2, {
          type: "pie",
          data: {
            labels: chartLabels,
            datasets: [{
              label: "Range 2",
              data: chartValues2,
              backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40", "#8BC34A", "#E91E63"]
            }]
          },
          options: { plugins: { title: { display: true, text: `${tableTitle} - Range 2` } } }
        });
      }
    }, 100);
  }
}


  container.innerHTML = html;
}
