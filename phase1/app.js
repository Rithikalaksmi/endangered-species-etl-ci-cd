/* ==========================================================================
   Endangered Species Analytics - Modern Dynamic Frontend App Engine
   ========================================================================== */

const API_BASE = '';

// Global State
let chartInstances = {};
let leafletMap = null;
let mapMarkersGroup = null;

document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initModalEvents();
    initControlButtons();
    initCubeFilters();
    
    // Initial Load
    refreshDashboard();
    
    // Auto Refresh every 30 seconds for live streaming feel
    setInterval(refreshDashboard, 30000);
});

// Toast Notifications
function showToast(message, type = 'success') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    const icon = type === 'success' ? 'fa-circle-check' : 'fa-triangle-exclamation';
    toast.innerHTML = `<i class="fa-solid ${icon}"></i> <span>${message}</span>`;
    
    container.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// Navigation & Tab Switching
function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item');
    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const targetTab = item.getAttribute('data-tab');
            
            navItems.forEach(i => i.classList.remove('active'));
            item.classList.add('active');
            
            document.querySelectorAll('.tab-content').forEach(tab => {
                tab.classList.remove('active');
            });
            
            const selectedTab = document.getElementById(`tab-${targetTab}`);
            if (selectedTab) {
                selectedTab.classList.add('active');
            }
            
            if (targetTab === 'gis-map' && leafletMap) {
                setTimeout(() => leafletMap.invalidateSize(), 200);
            }
        });
    });
}

// Modal Events
function initModalEvents() {
    const modal = document.getElementById('insert-modal');
    const openBtn = document.getElementById('open-insert-modal-btn');
    const closeBtn = document.getElementById('close-modal-btn');
    const cancelBtn = document.getElementById('cancel-modal-btn');
    const form = document.getElementById('insert-obs-form');

    const openModal = () => modal.classList.add('active');
    const closeModal = () => modal.classList.remove('active');

    openBtn.addEventListener('click', openModal);
    closeBtn.addEventListener('click', closeModal);
    cancelBtn.addEventListener('click', closeModal);

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        const payload = {
            common_name: document.getElementById('form-common-name').value,
            animal_category: document.getElementById('form-category').value,
            observed_on: document.getElementById('form-date').value,
            season: document.getElementById('form-season').value,
            latitude: parseFloat(document.getElementById('form-lat').value),
            longitude: parseFloat(document.getElementById('form-lon').value),
            place_guess: document.getElementById('form-place').value,
            description: document.getElementById('form-desc').value,
            quality_grade: 'research',
            media_score: 5
        };

        try {
            const res = await fetch(`${API_BASE}/api/observations/insert`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            
            closeModal();
            showToast('New species sighting inserted & ETL pipeline executed!');
            refreshDashboard();
        } catch (err) {
            showToast('Error inserting observation payload', 'error');
        }
    });
}

// Control Panel Actions
function initControlButtons() {
    document.getElementById('run-full-etl-btn').addEventListener('click', () => triggerPipelineRun('full', 'Full ETL Batch Pipeline executed!'));
    document.getElementById('run-incremental-btn').addEventListener('click', () => triggerPipelineRun('incremental', 'Incremental ETL Load completed!'));
    document.getElementById('run-api-ingest-btn').addEventListener('click', () => triggerPipelineRun('api', 'Simulated REST API Ingestion completed!'));
    
    document.getElementById('test-bad-data-btn').addEventListener('click', async () => {
        try {
            const res = await fetch(`${API_BASE}/api/observations/test-bad-data`, { method: 'POST' });
            const data = await res.json();
            showToast('Outlier rejected by Data Quality Gatekeeper & routed to Quarantine table!', 'warning');
            refreshDashboard();
        } catch (err) {
            showToast('Failed to trigger quarantine test', 'error');
        }
    });
}

async function triggerPipelineRun(mode, successMsg) {
    const badge = document.getElementById('pipeline-status-badge');
    badge.innerText = 'Pipeline Running...';
    
    try {
        const res = await fetch(`${API_BASE}/api/pipeline/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode: mode })
        });
        const data = await res.json();
        badge.innerText = 'Pipeline Active';
        showToast(successMsg);
        refreshDashboard();
    } catch (err) {
        badge.innerText = 'Pipeline Active';
        showToast(`Failed to execute ${mode} pipeline`, 'error');
    }
}

// OLAP Data Cube Filters
function initCubeFilters() {
    const catFilter = document.getElementById('cube-category-filter');
    const seasonFilter = document.getElementById('cube-season-filter');

    const updateCube = () => fetchCubeData(catFilter.value, seasonFilter.value);

    catFilter.addEventListener('change', updateCube);
    seasonFilter.addEventListener('change', updateCube);
}

// Global Refresh
async function refreshDashboard() {
    await fetchKPIs();
    await fetchChartData();
    await fetchMapPoints();
    await fetchCubeData();
    await fetchCDCLogs();
    await fetchAuditLogs();
}

// Fetch KPIs
async function fetchKPIs() {
    try {
        const res = await fetch(`${API_BASE}/api/analytics/kpis`);
        const kpis = await res.json();
        
        document.getElementById('kpi-total-obs').innerText = kpis.total_observations.toLocaleString();
        document.getElementById('kpi-total-species').innerText = `${kpis.total_species} (${kpis.total_categories} Categories)`;
        document.getElementById('kpi-clean-rate').innerText = `${kpis.clean_data_rate}%`;
        document.getElementById('kpi-cdc-events').innerText = kpis.cdc_events_count.toLocaleString();
    } catch (err) {
        console.error('KPI Fetch Error:', err);
    }
}

// Render Charts
async function fetchChartData() {
    try {
        const res = await fetch(`${API_BASE}/api/analytics/charts`);
        const data = await res.json();
        
        renderCategoryChart(data.categories);
        renderSeasonalChart(data.seasons);
        renderIUCNChart(data.iucn);
        renderQualityChart(data.quality);
    } catch (err) {
        console.error('Chart Data Fetch Error:', err);
    }
}

function renderCategoryChart(categories) {
    const ctx = document.getElementById('categoryChart').getContext('2d');
    if (chartInstances.category) chartInstances.category.destroy();

    const labels = categories.map(c => c.category);
    const counts = categories.map(c => c.count);

    chartInstances.category = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: counts,
                backgroundColor: [
                    '#10b981', '#06b6d4', '#8b5cf6', '#f59e0b', '#f43f5e',
                    '#3b82f6', '#ec4899', '#14b8a6', '#a855f7', '#64748b'
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'right', labels: { color: '#9ca3af', font: { family: 'Outfit', size: 11 } } }
            }
        }
    });
}

function renderSeasonalChart(seasons) {
    const ctx = document.getElementById('seasonalChart').getContext('2d');
    if (chartInstances.seasonal) chartInstances.seasonal.destroy();

    chartInstances.seasonal = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: seasons.map(s => s.season),
            datasets: [{
                label: 'Observations',
                data: seasons.map(s => s.count),
                backgroundColor: 'rgba(6, 182, 212, 0.7)',
                borderColor: '#06b6d4',
                borderWidth: 1,
                borderRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: '#9ca3af' }, grid: { display: false } },
                y: { ticks: { color: '#9ca3af' }, grid: { color: 'rgba(255,255,255,0.05)' } }
            },
            plugins: { legend: { display: false } }
        }
    });
}

function renderIUCNChart(iucn) {
    const ctx = document.getElementById('iucnChart').getContext('2d');
    if (chartInstances.iucn) chartInstances.iucn.destroy();

    chartInstances.iucn = new Chart(ctx, {
        type: 'polarArea',
        data: {
            labels: iucn.map(i => i.status),
            datasets: [{
                data: iucn.map(i => i.count),
                backgroundColor: [
                    'rgba(244, 63, 94, 0.7)',
                    'rgba(245, 158, 11, 0.7)',
                    'rgba(16, 185, 129, 0.7)'
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { r: { ticks: { display: false }, grid: { color: 'rgba(255,255,255,0.08)' } } },
            plugins: { legend: { position: 'right', labels: { color: '#9ca3af' } } }
        }
    });
}

function renderQualityChart(quality) {
    const ctx = document.getElementById('qualityChart').getContext('2d');
    if (chartInstances.quality) chartInstances.quality.destroy();

    chartInstances.quality = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: quality.map(q => q.grade),
            datasets: [{
                label: 'Avg Media Score',
                data: quality.map(q => q.avg_media_score),
                backgroundColor: 'rgba(139, 92, 246, 0.7)',
                borderColor: '#8b5cf6',
                borderWidth: 1,
                borderRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: '#9ca3af' }, grid: { display: false } },
                y: { ticks: { color: '#9ca3af' }, grid: { color: 'rgba(255,255,255,0.05)' }, max: 5 }
            },
            plugins: { legend: { display: false } }
        }
    });
}

// Leaflet GIS Map
async function fetchMapPoints() {
    try {
        const res = await fetch(`${API_BASE}/api/analytics/map?limit=250`);
        const points = await res.json();
        
        if (!leafletMap) {
            leafletMap = L.map('map').setView([20.5937, 78.9629], 5); // India center
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; OpenStreetMap &copy; CARTO',
                maxZoom: 18
            }).addTo(leafletMap);
            
            mapMarkersGroup = L.layerGroup().addTo(leafletMap);
        }

        mapMarkersGroup.clearLayers();

        points.forEach(pt => {
            const marker = L.circleMarker([pt.latitude, pt.longitude], {
                radius: 6,
                fillColor: getCategoryColor(pt.animal_category),
                color: '#ffffff',
                weight: 1,
                opacity: 0.9,
                fillOpacity: 0.8
            });

            const popupContent = `
                <div style="font-family: Outfit, sans-serif; color: #111;">
                    <strong style="font-size: 1rem; color: #10b981;">${pt.common_name}</strong><br/>
                    <small><b>Category:</b> ${pt.animal_category}</small><br/>
                    <small><b>IUCN:</b> ${pt.iucn_status}</small><br/>
                    <small><b>Location:</b> ${pt.place_guess}</small><br/>
                    <small><b>Date:</b> ${pt.observed_on} (${pt.season})</small>
                </div>
            `;
            marker.bindPopup(popupContent);
            mapMarkersGroup.addLayer(marker);
        });

    } catch (err) {
        console.error('GIS Map Fetch Error:', err);
    }
}

function getCategoryColor(cat) {
    switch (cat) {
        case 'Asian Elephant': return '#10b981';
        case 'Bengal tiger': return '#f59e0b';
        case 'One-Horned Rhinoceros': return '#06b6d4';
        case 'White-rumped Vulture': return '#f43f5e';
        case 'Nilgiri Tahr': return '#8b5cf6';
        case 'Gharial': return '#3b82f6';
        default: return '#ec4899';
    }
}

// Fetch OLAP Data Cube
async function fetchCubeData(cat = 'All', season = 'All') {
    try {
        const res = await fetch(`${API_BASE}/api/analytics/cube?category=${encodeURIComponent(cat)}&season=${encodeURIComponent(season)}`);
        const rows = await res.json();
        
        const tbody = document.getElementById('cube-table-body');
        if (rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="text-center">No Data Cube aggregation records matching slice filters.</td></tr>';
            return;
        }

        tbody.innerHTML = rows.map(r => `
            <tr>
                <td><strong>${r.category}</strong></td>
                <td><span class="badge badge-info">${r.season}</span></td>
                <td>${r.state}</td>
                <td><span class="badge badge-success">${r.quality_grade}</span></td>
                <td><strong>${r.total_observations.toLocaleString()}</strong></td>
                <td>${r.avg_media_score} / 5</td>
                <td>${r.has_image_count}</td>
                <td>${r.research_grade_count}</td>
            </tr>
        `).join('');

    } catch (err) {
        console.error('Data Cube Fetch Error:', err);
    }
}

// Fetch CDC Logs
async function fetchCDCLogs() {
    try {
        const res = await fetch(`${API_BASE}/api/cdc/logs`);
        const logs = await res.json();
        
        const tbody = document.getElementById('cdc-table-body');
        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="text-center">No CDC events logged yet.</td></tr>';
            return;
        }

        tbody.innerHTML = logs.map(l => {
            const badgeClass = l.change_type === 'INSERT' ? 'badge-success' : 'badge-warning';
            return `
                <tr>
                    <td><code>#${l.cdc_id}</code></td>
                    <td><code>${l.record_id}</code></td>
                    <td>${l.table_name}</td>
                    <td><span class="badge ${badgeClass}">${l.change_type}</span></td>
                    <td>${l.changed_fields}</td>
                    <td style="max-width:240px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;"><code>${l.new_values}</code></td>
                    <td><small>${l.timestamp}</small></td>
                </tr>
            `;
        }).join('');

    } catch (err) {
        console.error('CDC Logs Fetch Error:', err);
    }
}

// Fetch Audit Logs
async function fetchAuditLogs() {
    try {
        const res = await fetch(`${API_BASE}/api/pipeline/status`);
        const runs = await res.json();
        
        const tbody = document.getElementById('audit-table-body');
        if (runs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="10" class="text-center">No ETL pipeline runs recorded.</td></tr>';
            return;
        }

        tbody.innerHTML = runs.map(r => `
            <tr>
                <td><code>#${r.run_id}</code></td>
                <td><strong>${r.run_type}</strong></td>
                <td><span class="badge badge-success">${r.status}</span></td>
                <td>${r.records_processed}</td>
                <td>${r.records_valid}</td>
                <td><span class="badge badge-warning">${r.records_quarantined}</span></td>
                <td>${r.records_inserted}</td>
                <td>${r.records_updated}</td>
                <td>${r.duration_seconds}s</td>
                <td><small>${r.timestamp}</small></td>
            </tr>
        `).join('');

    } catch (err) {
        console.error('Audit Logs Fetch Error:', err);
    }
}
