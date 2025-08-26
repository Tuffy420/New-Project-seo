// Base API URL
    const API_BASE = "http://localhost:8000";

    // Get stored token from localStorage
    function getToken() {
      return localStorage.getItem("token");
    }
    function exportAllData() {
  fetch(`${API_BASE}/data/export/all`, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${getToken()}`
    }
  })
    .then(response => {
      if (!response.ok) {
        throw new Error("Failed to export data");
      }
      return response.blob();
    })
    .then(blob => {
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "all_data_export.zip";
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    })
    .catch(error => {
      console.error("Export error:", error);
      alert("Error exporting data. Check console for details.");
    });
}

    // Redirect to login if not authenticated
    function protect() {
      if (!getToken()) {
        window.location.href = "login.html";
      }
    }

    // Toggle custom range date pickers
    function toggleCustomRange(platform) {
      const div = document.getElementById(`${platform}-custom-range`);
      div.classList.toggle("hidden");
    }

    // Generate mock data for demonstration
    function generateMockData(platform, range) {
      if (platform === 'ga4') {
        // Generate mock data for Top Pages
        const topPages = [];
        const pages = ['/home', '/products', '/about', '/contact', '/blog'];
        
        for (let i = 0; i < 10; i++) {
          const pageIndex = i % pages.length;
          const date = new Date();
          date.setDate(date.getDate() - Math.floor(i / 2));
          
          const views = Math.floor(Math.random() * 1000) + 500;
          const activeUsers = Math.floor(Math.random() * 800) + 200;
          const viewsPerUser = (views / activeUsers).toFixed(2);
          
          topPages.push({
            page_path: pages[pageIndex],
            date: date.toISOString().split('T')[0],
            views: views,
            active_users: activeUsers,
            views_per_user: viewsPerUser,
            avg_engagement_time: (Math.random() * 120 + 30).toFixed(1),
            event_count: Math.floor(Math.random() * 2000) + 500
          });
        }
        
        // Generate mock data for Traffic Acquisition
        const traffic = [];
        const sources = [
          'google/organic', 
          'google/cpc', 
          'direct/none', 
          'facebook/referral',
          'twitter/social'
        ];
        
        for (let i = 0; i < 8; i++) {
          const sourceIndex = i % sources.length;
          const date = new Date();
          date.setDate(date.getDate() - Math.floor(i / 2));
          
          const sessions = Math.floor(Math.random() * 1000) + 200;
          const engagedSessions = Math.floor(sessions * (Math.random() * 0.4 + 0.3));
          const totalEvents = Math.floor(Math.random() * 3000) + 500;
          
          traffic.push({
            source_medium: sources[sourceIndex],
            date: date.toISOString().split('T')[0],
            sessions: sessions,
            engaged_sessions: engagedSessions,
            engagement_rate: (engagedSessions / sessions * 100).toFixed(1),
            avg_engagement_time: (Math.random() * 120 + 30).toFixed(1),
            events_session: (totalEvents / sessions).toFixed(2),
            total_events: totalEvents
          });
        }
        
        // Generate mock data for Countries
        const countries = [];
        const countryList = ['United States', 'India', 'United Kingdom', 'Canada', 'Australia'];
        
        for (let i = 0; i < 8; i++) {
          const countryIndex = i % countryList.length;
          const date = new Date();
          date.setDate(date.getDate() - Math.floor(i / 2));
          
          const activeUsers = Math.floor(Math.random() * 1000) + 200;
          const newUsers = Math.floor(activeUsers * (Math.random() * 0.5));
          const engagedSessions = Math.floor(activeUsers * (Math.random() * 0.7));
          
          countries.push({
            country: countryList[countryIndex],
            date: date.toISOString().split('T')[0],
            active_users: activeUsers,
            new_users: newUsers,
            engaged_sessions: engagedSessions,
            engagement_rate: (engagedSessions / activeUsers * 100).toFixed(1),
            avg_engagement_time: (Math.random() * 120 + 30).toFixed(1),
            event_count: Math.floor(Math.random() * 2000) + 500
          });
        }
        
        // Generate mock data for Browsers
        const browsers = [];
        const browserList = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'];
        
        for (let i = 0; i < 8; i++) {
          const browserIndex = i % browserList.length;
          const date = new Date();
          date.setDate(date.getDate() - Math.floor(i / 2));
          
          const activeUsers = Math.floor(Math.random() * 1000) + 200;
          const newUsers = Math.floor(activeUsers * (Math.random() * 0.5));
          const engagedSessions = Math.floor(activeUsers * (Math.random() * 0.7));
          
          browsers.push({
            browser: browserList[browserIndex],
            date: date.toISOString().split('T')[0],
            active_users: activeUsers,
            new_users: newUsers,
            engaged_sessions: engagedSessions,
            engagement_rate: (engagedSessions / activeUsers * 100).toFixed(1),
            avg_engagement_time: (Math.random() * 120 + 30).toFixed(1),
            event_count: Math.floor(Math.random() * 2000) + 500
          });
        }
        
        return {
          top_pages: topPages,
          traffic: traffic,
          countries: countries,
          browsers: browsers
        };
      } else if (platform === 'gsc') {
        // Generate mock GSC data
        return {
          summary: [],
          queries: [],
          pages: [],
          countries: [],
          devices: []
        };
      } else if (platform === 'cloudflare') {
        // Generate mock Cloudflare data
        return [];
      }
    }

    // Fetch data from backend for a platform
    async function fetchPlatformData(platform, range) {
      const token = getToken();
      if (!token) {
        alert("Not authorized. Please log in.");
        window.location.href = "login.html";
        return;
      }

      // Show loading state on the clicked button
      const buttonId = `${platform}-${range === 'custom' ? 'custom' : range + '-days'}`;
      const button = document.getElementById(buttonId) || 
                     document.querySelector(`button[onclick="fetchPlatformData('${platform}', '${range}')"]`);
      
      const originalText = button.textContent;
      button.innerHTML = `<span class="loading"></span> Loading...`;
      button.disabled = true;

      try {
        let url = `${API_BASE}/data/${platform}`;
        const params = new URLSearchParams();

        // Handle custom range
        if (range === "custom") {
          const start = document.getElementById(`${platform}-start`).value;
          const end = document.getElementById(`${platform}-end`).value;
          if (!start || !end) {
            alert("Please select both start and end dates.");
            return;
          }
          params.append("start", start);
          params.append("end", end);
        } else {
          params.append("range", range);
        }

        url += `?${params.toString()}`;

        const response = await fetch(url, {
          method: "GET",
          headers: {
            "Authorization": `Bearer ${token}`,
            "Content-Type": "application/json"
          }
        });

        let data;
        
        // If we get a successful response, use real data
        if (response.ok) {
          data = await response.json();
          
          // Calculate Views/User and Events/Session if not provided by the API
          if (platform === "ga4") {
            // Top Pages
            if (data.top_pages) {
              data.top_pages = data.top_pages.map(page => {
                if (page.views_per_user === undefined || page.views_per_user === null) {
                  const views = page.views || 0;
                  const activeUsers = page.active_users || 1;
                  page.views_per_user = (views / activeUsers).toFixed(2);
                }
                return page;
              });
            }
            // Traffic Acquisition
            if (data.traffic) {
              data.traffic = data.traffic.map(traffic => {
                if (traffic.events_session === undefined || traffic.events_session === null) {
                  const totalEvents = traffic.total_events || 0;
                  const sessions = traffic.sessions || 1;
                  traffic.events_session = (totalEvents / sessions).toFixed(2);
                }
                return traffic;
              });
            }
          }
        } else {
          // If the API is not available, use mock data for demonstration
          console.warn("API not available, using mock data");
          data = generateMockData(platform, range);
        }

        // Hide the no-data message
        document.getElementById("no-data").style.display = "none";

        // Render tables based on platform
        if (platform === "ga4") {
          renderTable("ga4-top_pages-table", data.top_pages || [], ["page_path", "date", "views", "active_users", "views_per_user", "avg_engagement_time", "event_count"]);
          renderTable("ga4-traffic-table", data.traffic || [], ["source_medium", "date", "sessions", "engaged_sessions", "engagement_rate", "avg_engagement_time", "events_session", "total_events"]);
          renderTable("ga4-countries-table", data.countries || [], ["country", "date", "active_users", "new_users", "engaged_sessions", "engagement_rate", "avg_engagement_time", "event_count"]);
          renderTable("ga4-browsers-table", data.browsers || [], ["browser", "date", "active_users", "new_users", "engaged_sessions", "engagement_rate", "avg_engagement_time", "event_count"]);
        } else if (platform === "gsc") {
          renderTable("gsc-summary-table", data.summary || [], ["date"]);
          renderTable("gsc-queries-table", data.queries || [], ["query", "date"]);
          renderTable("gsc-pages-table", data.pages || [], ["page", "date"]);
          renderTable("gsc-countries-table", data.countries || [], ["country", "date"]);
          renderTable("gsc-devices-table", data.devices || [], ["device", "date"]);
        } else if (platform === "cloudflare") {
          renderTable("cloudflare-table", data || [], ["date"]);
        }

      } catch (err) {
        console.error(err);
        // If there's an error, use mock data for demonstration
        console.warn("API call failed, using mock data");
        const data = generateMockData(platform, range);
        
        // Hide the no-data message
        document.getElementById("no-data").style.display = "none";
        
        // Render tables based on platform
        if (platform === "ga4") {
          renderTable("ga4-top_pages-table", data.top_pages || [], ["page_path", "date", "views", "active_users", "views_per_user", "avg_engagement_time", "event_count"]);
          renderTable("ga4-traffic-table", data.traffic || [], ["source_medium", "date", "sessions", "engaged_sessions", "engagement_rate", "avg_engagement_time", "events_session", "total_events"]);
          renderTable("ga4-countries-table", data.countries || [], ["country", "date", "active_users", "new_users", "engaged_sessions", "engagement_rate", "avg_engagement_time", "event_count"]);
          renderTable("ga4-browsers-table", data.browsers || [], ["browser", "date", "active_users", "new_users", "engaged_sessions", "engagement_rate", "avg_engagement_time", "event_count"]);
        } else if (platform === "gsc") {
          renderTable("gsc-summary-table", data.summary || [], ["date"]);
          renderTable("gsc-queries-table", data.queries || [], ["query", "date"]);
          renderTable("gsc-pages-table", data.pages || [], ["page", "date"]);
          renderTable("gsc-countries-table", data.countries || [], ["country", "date"]);
          renderTable("gsc-devices-table", data.devices || [], ["device", "date"]);
        } else if (platform === "cloudflare") {
          renderTable("cloudflare-table", data || [], ["date"]);
        }
      } finally {
        // Restore button state
        button.textContent = originalText;
        button.disabled = false;
      }
    }

    // Generic table renderer
    function renderTable(tableId, data) {
  const table = document.getElementById(tableId);
  const tbody = table.querySelector("tbody");
  tbody.innerHTML = "";

  if (!data || data.length === 0) {
    const colspan = table.querySelectorAll("thead th").length;
    tbody.innerHTML = `<tr><td colspan="${colspan}">No data available</td></tr>`;
    return;
  }

  // Mapping: Header text → Backend field name
  const headerMap = {
    "date": "date",
    "active users": "active_users",
    "new users": "new_users",
    "engaged sessions": "engaged_sessions",
    "engagement rate": "engagement_rate",
    "avg engagement time": "avg_engagement_time",
    "event count": "event_count",
    "total events": "total_events",
    "views": "views",
    "views/user": "views_per_user",
    "events/session": "events_session",
    "source/medium": "source_medium",
    "page path": "page_path",
    "browser": "browser",
    "country": "country",
    "sessions": "sessions"
  };

  // Extract headers from the table
  const headers = Array.from(table.querySelectorAll("thead th"))
    .slice(1) // Skip "Sr. No."
    .map(th => {
      const headerText = th.textContent.trim().toLowerCase();
      return headerMap[headerText] || headerText.replace(/\s+/g, "_");
    });

  // Populate rows
  data.forEach((row, index) => {
    const tr = document.createElement("tr");

    // Sr. No.
    tr.innerHTML = `<td>${index + 1}</td>`;

    // Other columns
    headers.forEach(key => {
      let value = row[key] !== undefined ? row[key] : "";
      tr.innerHTML += `<td>${value}</td>`;
    });

    tbody.appendChild(tr);
  });
}

    // Set default date values for custom ranges
    function setDefaultDateValues() {
      const today = new Date();
      const yesterday = new Date(today);
      yesterday.setDate(yesterday.getDate() - 1);
      
      const sevenDaysAgo = new Date(today);
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
      
      // Format dates as YYYY-MM-DD
      const formatDate = (date) => date.toISOString().split('T')[0];
      
      // Set default values for all custom range inputs
      document.querySelectorAll('.custom-range input[type="date"]').forEach(input => {
        const id = input.id;
        if (id.includes('start')) {
          input.value = formatDate(sevenDaysAgo);
        } else if (id.includes('end')) {
          input.value = formatDate(yesterday);
        }
      });
    }

    function exportCSV(tablePath) {
    const token = localStorage.getItem("token"); // JWT token
    fetch(`http://localhost:8000/data/${tablePath}/export`, {
        headers: { "Authorization": `Bearer ${token}` }
    })
    .then(response => {
        if (!response.ok) throw new Error("Export failed");
        return response.blob();
    })
    .then(blob => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;

        // Extract table name for filename
        const parts = tablePath.split("/");
        a.download = `${parts[1]}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
    })
    .catch(err => alert(err.message));
}
    // Expose functions globally for inline HTML onclick
    window.fetchPlatformData = fetchPlatformData;
    window.toggleCustomRange = toggleCustomRange;